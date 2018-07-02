{-# LANGUAGE OverloadedStrings, GADTs, BangPatterns #-}

module Main where

import Lib
import Control.Arrow
import Control.Monad.State.Lazy

import Prelude hiding (reads, putStr)
import Data.List (head, map, foldl')
import Data.Tuple (swap)
import Data.Monoid (Sum(..), getSum, (<>))

-- options parsing
import Options.Applicative (execParser, info, helper,
                            fullDesc, progDesc, header,
                            strOption, long, short, help,
                            (<*>), (<**>))
import qualified Options.Applicative as OptParse

-- trace parsing
import Text.Megaparsec hiding (Parser, State)
import Text.Megaparsec.Byte
import Data.ByteString.Conversion (fromByteString)
import Data.Void (Void)
import Numeric (readDec, readHex)
import qualified Data.List.NonEmpty as NE
import qualified Data.Set as Set

-- gzip decompression
import qualified Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy.Char8 as LBC -- required by GZip
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC

-- DOT format out
import qualified Data.Graph.Inductive.Graph as Graph
import Data.Graph.Inductive.Graph (Node, LNode, Edge, LEdge, UEdge,
                                   insNode, insEdge, prettyPrint)
import Data.Graph.Inductive.PatriciaTree (Gr)
import Data.GraphViz (GraphvizParams, graphToDot, defaultParams, )
import Data.GraphViz.Types (printDotGraph)
import Data.Text.Lazy.IO (putStr)

---------------------------------------------------------------------
-- Trace representation
data StEvent = Sync { ty     :: !Int,
                      addr   :: !Int }
             | Comm { bytes  :: !Int }
             | Comp { iops   :: !Int,
                      flops  :: !Int,
                      reads  :: !Int,
                      writes :: !Int }
             | End -- simplify parsing; eventually we'll parse the EOF and have to return a StEvent
             deriving (Eq, Show, Ord)
type StTrace = [StEvent]

---------------------------------------------------------------------
-- Parse command line args
data Args = Args { input :: String, output :: String }

parseArgs :: OptParse.Parser Args
parseArgs = Args
    <$> strOption
        ( long "input"
        <> short 'i'
        <> help "Source directory of SynchroTrace traces" )
    <*> strOption
        ( long "output"
        <> short 'o'
        <> help "Destination of DOT-formatted graph" )

---------------------------------------------------------------------
-- Read a gzipped trace
readGz :: FilePath -> IO [LBC.ByteString]
readGz = LBC.readFile >=> GZip.decompress >>> LBC.split '\n' >>> return

---------------------------------------------------------------------
-- Parse the input trace
type MegaParser = Parsec Void LBC.ByteString

-- These definitions are required because we are working with Word8's.
-- If there was an implicit conversion from Char to Word8,
-- we could just use e.g. ':' instead of 58 :: Word8.
zero = 48
one = zero + 1
two = zero + 2
three = zero + 3
four = zero + 4
five = zero + 5
six = zero + 6
seven = zero + 7
eight = zero + 8
nine = zero + 9
comma = 44
colon = 58
caret = 94
pound = 35
nlchar = 10
pth_ty = ("pth_ty" :: LBC.ByteString)
hexHeader = ("0x" :: LBC.ByteString)

-- 'shrugs' Just want to note we expect digits
digitErr :: Set.Set (ErrorItem (Token LBC.ByteString))
digitErr = Set.singleton $ Tokens $ zero NE.:| [one, two, three, four, five, six, seven, eight, nine]

digitChars = some digitChar
hexDigitChars = string hexHeader >> some hexDigitChar

-- TODO should probably do some checking here >.<
parseDec :: MegaParser Int
parseHex :: MegaParser Int
parseDec = (fst . head . readDec . BC.unpack . B.pack) <$> digitChars
parseHex = (fst . head . readHex . BC.unpack . B.pack) <$> hexDigitChars

readDecFromCSVs :: MegaParser [Int]
readDecFromCSVs = (fromByteString . B.pack <$>) <$> (digitChars `sepBy` char comma) >>= intListOrErr
    where
        intListOrErr = sequence >>> maybe fail' return
        fail' = failure Nothing $ digitErr
        -- parse error, this is a bit redundant because we already fail
        -- on a pattern match failure later on

stEventHeader :: MegaParser (Int, Int)
stEventHeader = do
    eid <- space >> parseDec
    tid <- char comma >> parseDec
    return $ (eid, tid)

syncEvent :: MegaParser StEvent
syncEvent = do
    char comma >> string pth_ty >> char colon
    ty <- parseDec
    addr <- char caret >> parseHex
    many anyChar
    return $ Sync ty addr

compEvent :: MegaParser StEvent
compEvent = do
    char comma
    [iops, flops, reads, writes] <- readDecFromCSVs
    many anyChar
    return $ Comp iops flops reads writes

-- Communication events require additional logic because there is no
-- total reads or total bytes read field.
-- In this case we only care about total bytes read.
commEdge :: MegaParser (Sum Int)
commEdge = do
    space >> char pound >> space >> digitChars >> space >> digitChars >> space
    from <- parseHex
    to <- space >> parseHex
    return $ Sum (to - from + 1)

commEvent :: MegaParser StEvent
commEvent = do
    edges <- some $ try commEdge
    many anyChar
    return $ Comm $ getSum $ mconcat edges

endEvent :: MegaParser StEvent
endEvent = eof >> return End

stEvent :: MegaParser StEvent
stEvent = try (stEventHeader >>
               (try compEvent <|> try syncEvent <|> commEvent))
          <|> endEvent

parseEvent :: LBC.ByteString -> StEvent
parseEvent = parse stEvent "" >>> either errorOut id
    where
        errorOut err = error $ show err
        -- bail out and error on a parse error; no point in continuing

parseTrace :: [LBC.ByteString] -> StTrace
parseTrace = map parseEvent

---------------------------------------------------------------------
-- Generate a graph from event trace

newtype StContainedEvents = StContainedEvents (Bool, Bool) deriving (Show)
data StNodeData = StNodeData { syncTy       :: !Int,
                               syncAddr     :: !Int,
                               aggIops      :: !Int,
                               aggFlops     :: !Int,
                               aggReads     :: !Int,
                               aggWrites    :: !Int,
                               aggCommBytes :: !Int } deriving (Show)
-- Aggregate of Sync/Comm/Comp event types

type StNode = LNode StNodeData
type StEdge = UEdge -- Labels for edges are not required for current use case
type StGraph = Gr StNodeData ()
type StGraph' = (StGraph, StNode)
-- Graph types
-- We use a tuple of (normal graph, last node) to track state because
-- we try to merge each new 'event' into 'last node' to prevent the graph
-- from becoming unwieldy.
-- (and to prevent it from just looking like the text trace).

graphTrace :: StTrace -> StGraph
graphTrace = graphTrace' >>> swap >>> uncurry insNode

graphTrace' :: StTrace -> StGraph'
graphTrace' = foldl' insEvent initGr
    where
        firstNode = makeStNode 0 (Sync 0 0) -- start with any 'empty' event
        initGr = (Graph.empty, firstNode)

insEvent :: StGraph' -> StEvent -> StGraph'
insEvent gr' End = gr'
insEvent (!gr, last@(!node, !_)) event = maybe default' setNode $ tryMerge last event
    -- NB Free up memory by forcing evaluation of the inner pair elements
    where
        setNode = (,) gr
        nextNode = succ node
        nextStEdge = (node, nextNode, ())
        nextStNode = makeStNode nextNode event
        nextGraph = insEdge nextStEdge $ insNode last gr
        default' = (nextGraph, nextStNode)

makeStNode :: Node -> StEvent -> StNode
-- TODO Do we ever start from a Comp or Comm event?
-- YES, traces don't start with a default 'start thread' event.
-- Maybe we should add one.
makeStNode n (Comp i f r w) = (n, StNodeData 0 0 i f r w 0)
makeStNode n (Comm b) = (n, StNodeData 0 0 0 0 0 0 b)
makeStNode n (Sync t a) = (n, StNodeData t a 0 0 0 0 0)

-- TODO There must be a nicer way to do this...this looks quite error prone.
-- Only Comm and Comp events are considered for merging.
-- NB Sync events will always *start* a new node in this implementation.
tryMerge :: StNode -> StEvent -> Maybe StNode
tryMerge (node, nodeData) (Comp i f r w) = Just $ (node, merged)
    where
        merged = StNodeData (syncTy   nodeData)
                            (syncAddr nodeData)
                            (i + aggIops   nodeData)
                            (f + aggFlops  nodeData)
                            (r + aggReads  nodeData)
                            (w + aggWrites nodeData)
                            (aggCommBytes nodeData)
tryMerge (node, nodeData) (Comm b) = Just $ (node, merged)
    where
        merged = StNodeData (syncTy    nodeData)
                            (syncAddr  nodeData)
                            (aggIops   nodeData)
                            (aggFlops  nodeData)
                            (aggReads  nodeData)
                            (aggWrites nodeData)
                            (b + aggCommBytes nodeData)
tryMerge _ _ = Nothing -- can't merge Sync events

---------------------------------------------------------------------
-- Write out to GraphViz

defaultParams' :: GraphvizParams Node StNodeData () Int StNodeData
defaultParams' = defaultParams

---------------------------------------------------------------------
-- Main
main :: IO ()
main = execParser opts >>= \(Args i _) -> readGz i >>= printDot
    where
        opts = info (parseArgs <**> helper)
            (fullDesc <> progDesc "Read a gz" <> header "stgraph - graph")

printTrace :: [LBC.ByteString] -> IO ()
printTrace = parseTrace >>> mapM_ print

printGraph :: [LBC.ByteString] -> IO ()
printGraph = parseTrace >>> graphTrace >>> prettyPrint

printDot :: [LBC.ByteString] -> IO ()
printDot = parseTrace >>> graphTrace >>> graphToDot defaultParams' >>> printDotGraph >>> putStr