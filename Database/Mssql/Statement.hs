module Database.Mssql.Statement where
import Database.Mssql.Tds
import Database.Mssql.Collation

import Control.Concurrent.MVar
import Data.Bits
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
             metadatatok :: MVar Token,
             bufSize :: Int}

newSth :: Handle -> Int -> String -> IO Statement
newSth conn bufSize query =
    do newcoldefmv <- newMVar []
       tokenstm <- newMVar []
       metadatatok <- newMVar TokColMetaDataEmpty
       let sstate = SState {conn = conn, squery = query,
                            coldefmv = newcoldefmv,
                            tokenstm = tokenstm,
                            metadatatok = metadatatok,
                            bufSize = bufSize}
           retval = Statement {executeRaw = fexecuteRaw sstate,
                               execute = fexecute sstate,
                               executeMany = fexecuteMany sstate,
                               getColumnNames = fgetColumnNames sstate,
                               fetchRow = ffetchRow sstate,
                               originalQuery = foriginalQuery sstate,
                               describeResult = fdescribeResult sstate,
                               finish = ffinish sstate}
       return retval

colDescFromTi :: TypeInfo -> SqlColDesc
colDescFromTi TypeInt4 = SqlColDesc SqlIntegerT Nothing Nothing Nothing (Just False)
colDescFromTi TypeFlt4 = SqlColDesc SqlFloatT Nothing Nothing Nothing (Just False)
colDescFromTi TypeFlt8 = SqlColDesc SqlDoubleT Nothing Nothing Nothing (Just False)
colDescFromTi (TypeFltN 4) = SqlColDesc SqlFloatT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeFltN 8) = SqlColDesc SqlDoubleT Nothing Nothing Nothing (Just True)
colDescFromTi (TypeDecimalN prec scale) = SqlColDesc SqlDecimalT (Just (fromIntegral prec)) Nothing (Just (fromIntegral scale)) (Just True)
colDescFromTi (TypeNumericN prec scale) = SqlColDesc SqlDecimalT (Just (fromIntegral prec)) Nothing (Just (fromIntegral scale)) (Just True)

processResp :: [Token] -> [Token] -> (Maybe Token, [Token], [Token], Bool)
processResp (metadata@(TokColMetaData _ _):xs) errors =
    (Just metadata, xs, [], True)
processResp (err@(TokError _ _ _ _ _ _ _):xs) errors =
    processResp xs (err:errors)
processResp (done@(TokDone status _ _):xs) errors =
    if status .&. doneMoreResults == 0
         then if xs == []
             then (Nothing, [], errors, isDoneSuccess done)
             else error "unexpected tokens after final DONE token"
         else processResp xs []

fexecuteRaw :: SState -> IO ()
fexecuteRaw sstate =
    do tokens <- exec (conn sstate) (squery sstate) (bufSize sstate)
       let (mmetadata, remtokens, errors, status) = processResp tokens []
       case mmetadata of
           Just metadata -> do
               let metadataCols (TokColMetaData cols _) = cols
                   cols = metadataCols metadata
                   coldef (ColMetaData usertype flags ti name) =
                       (name, colDescFromTi ti)
               swapMVar (coldefmv sstate) [coldef col | col <- cols]
               swapMVar (tokenstm sstate) remtokens
               swapMVar (metadatatok sstate) metadata
               return ()
           Nothing -> do
               throwSqlError (if errors == []
                   then SqlError {seState = "", seNativeError = -1, seErrorMsg = "Query failed, server didn't send an error"}
                   else let (TokError errno _ _ msg _ _ _) = head errors
                       in SqlError {seState = "", seNativeError = errno, seErrorMsg = msg})

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

fexecute :: SState -> [SqlValue] -> IO Integer
fexecute sstate params = return 0

fexecuteMany :: SState -> [[SqlValue]] -> IO ()
fexecuteMany sstate params = return ()

foriginalQuery :: SState -> String
foriginalQuery sstate = squery sstate

fdescribeResult :: SState -> IO [(String, SqlColDesc)]
fdescribeResult sstate = readMVar (coldefmv sstate)

ffinish :: SState -> IO ()
ffinish sstate = return ()
