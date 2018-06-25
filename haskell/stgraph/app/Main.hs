{-# LANGUAGE OverloadedStrings, GADTs, DuplicateRecordFields #-}

module Main where

import Lib
import Numeric
import Debug.Trace
import Data.Monoid

-- options parsing
import Options.Applicative hiding (Parser, many, some)
import qualified Options.Applicative as OptParse

-- trace parsing
import Text.Megaparsec hiding (Parser)
import Text.Megaparsec.Byte
import Data.Void
import Data.Word
import Data.ByteString.Conversion
import qualified Data.List.NonEmpty as NE
import qualified Data.Set as Set

-- gzip decompression
import qualified Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy.Char8 as LBC -- required by GZip
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC

-- DOT format out
--import Data.GraphViz

---------------------------------------------------------------------
-- Argument parsing
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
-- Reading in traces

-- for testing
readFirstLine :: FilePath -> IO LBC.ByteString
readFirstLine path = head <$> (readGz path)

readGz :: FilePath -> IO [LBC.ByteString]
readGz path = (LBC.split '\n' . GZip.decompress) <$> (LBC.readFile path)

---------------------------------------------------------------------
-- Parsing the trace
data StEvent = Sync { ty :: Int,  addr :: Int }
             | Comm { bytes :: Int }
             | Comp { iops :: Int, flops :: Int, reads :: Int, writes :: Int }
             | End -- simplify parsing; eventually we'll parse the EOF and have to return a StEvent
             deriving (Eq, Show)

type MegaParser = Parsec Void LBC.ByteString

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
readDecFromCSVs = (fromByteString . B.pack <$>) <$> (digitChars `sepBy` char comma) >>= intOrErr
    where
        intOrErr :: [Maybe Int] -> MegaParser [Int]
        intOrErr x = case sequence x of
                       Nothing -> failure Nothing $ digitErr
                       Just xs -> return xs

stEventHeader :: MegaParser (Int, Int)
stEventHeader = do
    space
    eid <- parseDec
    char comma
    tid <- parseDec
    return (eid, tid)

syncEvent :: MegaParser StEvent
syncEvent = do
    char comma >> string pth_ty >> char colon
    ty <- parseDec
    char caret
    addr <- parseHex
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
    space
    to <- parseHex
    return $ Sum (to - from + 1)

commEvent :: MegaParser StEvent
commEvent = do
    edges <- some $ try commEdge
    many anyChar
    return $ Comm $ getSum $ mconcat edges

endEvent :: MegaParser StEvent
endEvent = eof >> return End

stEvent :: MegaParser StEvent
stEvent = try (stEventHeader >> (try compEvent <|> try syncEvent <|> commEvent)) <|> endEvent

parseEvent :: LBC.ByteString -> StEvent
parseEvent bs = case parse stEvent "" bs of
                  -- bail out and error on a parse error,
                  -- no point in continuing
                  Left parseError -> error $ show parseError
                  Right event -> event

---------------------------------------------------------------------
-- Writing out to GraphViz

-- aggregate of different data types
data GvNode = GvNode { iops      :: Int,
                       flops     :: Int,
                       reads     :: Int,
                       writes    :: Int,
                       commBytes :: Int }
            deriving (Show)

---------------------------------------------------------------------
-- Main
main :: IO ()
main = execParser opts >>= printTrace
    where
        opts = info (parseArgs <**> helper)
            (fullDesc <> progDesc "Read a gz" <> header "stgraph - graph")

printTrace :: Args -> IO ()
printTrace (Args i o) = mapM_ (print . parseEvent) =<< readGz i
