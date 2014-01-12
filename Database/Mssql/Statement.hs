module Database.Mssql.Statement where
import Database.Mssql.Tds

import Control.Concurrent.MVar
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
convertVal (TdsInt1 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt2 v) = SqlInt32 (fromIntegral v)
convertVal (TdsInt4 v) = SqlInt32 v
convertVal (TdsInt8 v) = SqlInt64 v
convertVal (TdsFloat v) = SqlDouble v
convertVal (TdsReal v) = SqlDouble (float2Double v)
convertVal (TdsGuid v) = SqlByteString v
convertVal (TdsBool v) = SqlBool v
convertVal (TdsDecimal _ _ v) = SqlRational v

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
