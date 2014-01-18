module Database.Mssql.Statement where
import Database.Mssql.Tds
import Database.Mssql.Collation

import Control.Concurrent.MVar
import qualified Data.ByteString.Lazy as B
import qualified Data.Encoding as E
import Data.Encoding.UTF16
import Data.Ratio
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.LocalTime
import Database.HDBC
import Database.HDBC.Types
import System.IO
import GHC.Float

data SState =
    SState { conn :: Handle,
             squery :: String,
             coldefmv :: MVar [(String, SqlColDesc)],
             tokenstm :: MVar [Token],
             metadatatok :: MVar Token}

newSth :: Handle -> String -> IO Statement
newSth conn query =
    do newcoldefmv <- newMVar []
       tokenstm <- newMVar []
       metadatatok <- newMVar TokColMetaDataEmpty
       let sstate = SState {conn = conn, squery = query,
                            coldefmv = newcoldefmv,
                            tokenstm = tokenstm,
                            metadatatok = metadatatok}
           retval = Statement {executeRaw = fexecuteRaw sstate,
                               getColumnNames = fgetColumnNames sstate,
                               fetchRow = ffetchRow sstate}
       return retval


fexecuteRaw :: SState -> IO ()
fexecuteRaw sstate =
    do tokens <- exec (conn sstate) (squery sstate)
       let metadatas = filter isTokMetaData tokens
           metadata = head metadatas
           metadataCols (TokColMetaData cols _) = cols
           cols = metadataCols metadata
           coldef (ColMetaData usertype flags ti name) =
                (name, SqlColDesc SqlCharT Nothing Nothing Nothing Nothing)
       swapMVar (coldefmv sstate) [coldef col | col <- cols]
       swapMVar (tokenstm sstate) tokens
       swapMVar (metadatatok sstate) metadata
       return ()

convertVal :: TdsValue -> SqlValue
convertVal TdsNull = SqlNull
convertVal (TdsInt1 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt2 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt4 v) = SqlInt32 v
convertVal (TdsInt8 v) = SqlInt64 v
convertVal (TdsFloat v) = SqlDouble v
convertVal (TdsReal v) = SqlDouble (float2Double v)
convertVal (TdsGuid v) = SqlByteString v
convertVal (TdsBool v) = SqlBool v
convertVal (TdsDecimal _ _ v) = SqlRational v
convertVal (TdsMoney v) = SqlRational $ (fromIntegral v) % 10000
convertVal (TdsSmallMoney v) = SqlRational $ (fromIntegral v) % 10000
convertVal (TdsDateTime days timefrac) = SqlLocalTime time
    where day = addDays (fromIntegral days) (fromGregorian 1900 1 1)
          picoseconds = round $ ((fromIntegral timefrac) * 10^12) % 300
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds
          time = LocalTime day daytime

convertVal (TdsSmallDateTime days minutes) = SqlLocalTime time
    where day = addDays (fromIntegral days) (fromGregorian 1900 1 1)
          daytime = timeToTimeOfDay $ secondsToDiffTime ((fromIntegral minutes) * 60)
          time = LocalTime day daytime

convertVal (TdsDate days) = SqlLocalDate day
    where day = addDays (fromIntegral days) (fromGregorian 1 1 1)

convertVal (TdsTime secs) = SqlLocalTimeOfDay daytime
    where picoseconds = round $ secs * 10^12
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds

convertVal (TdsDateTime2 days secs) = SqlLocalTime $ LocalTime day daytime
    where day = addDays (fromIntegral days) (fromGregorian 1 1 1)
          picoseconds = round $ secs * 10^12
          daytime = timeToTimeOfDay $ picosecondsToDiffTime picoseconds

convertVal (TdsDateTimeOffset days secs offset) = res
    where startday = (fromGregorian 1 1 1)
          day = addDays (fromIntegral days) startday
          picoseconds = round $ secs * 10^12
          daytime = picosecondsToDiffTime picoseconds
          tz = minutesToTimeZone $ fromIntegral offset
          utctime = UTCTime day daytime
          zonedtime = utcToZonedTime tz utctime
          res = SqlZonedTime zonedtime

convertVal (TdsVarBinary bs) = SqlByteString bs
convertVal (TdsBinary bs) = SqlByteString bs
convertVal (TdsChar collation bs) = SqlString $ E.decodeStrictByteString (getCharSet collation) bs
convertVal (TdsVarChar collation bs) = SqlString $ E.decodeStrictByteString (getCharSet collation) bs
convertVal (TdsNChar collation bs) = SqlString $ E.decodeStrictByteString UTF16LE bs
convertVal (TdsNVarChar collation bs) = SqlString $ E.decodeStrictByteString UTF16LE bs
convertVal (TdsXml bs) = SqlString $ E.decodeLazyByteString UTF16LE bs
convertVal (TdsText collation bs) = SqlString $ E.decodeLazyByteString (getCharSet collation) bs
convertVal (TdsNText collation bs) = SqlString $ E.decodeLazyByteString UTF16LE bs
convertVal (TdsImage bs) = SqlByteString $ B.toStrict bs

convertVals :: [TdsValue] -> [SqlValue]
convertVals [] = []
convertVals (v:xs) = convertVal v : convertVals xs

decodeRow :: Token -> [SqlValue]
decodeRow (TokRow vals) = convertVals vals

fgetColumnNames :: SState -> IO [(String)]
fgetColumnNames sstate =
    do c <- readMVar (coldefmv sstate)
       return (map fst c)


ffetchRow :: SState -> IO (Maybe [SqlValue])
ffetchRow sstate =
    do metadata <- readMVar (metadatatok sstate)
       case metadata of
            TokColMetaData _ [] -> return Nothing
            TokColMetaData cols rows -> do
                let row = head rows
                    remrows = tail rows
                swapMVar (metadatatok sstate) (TokColMetaData cols remrows)
                return $ (Just . decodeRow) row
            otherwise -> return Nothing
