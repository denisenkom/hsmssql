module Database.Mssql.Statement where
import Database.Mssql.Tds

import Control.Concurrent.MVar
import Database.HDBC
import Database.HDBC.Types
import System.IO

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
       print 1
       swapMVar (coldefmv sstate) [coldef col | col <- cols]
       print 2
       swapMVar (tokenstm sstate) tokens
       print 3
       swapMVar (metadatatok sstate) metadata
       print 4
       return ()

convertVals :: [Int] -> [SqlValue]
convertVals [] = []
convertVals (val:xs) = (SqlInt32 . fromIntegral) val : convertVals xs

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
            TokColMetaData _ rows -> (return . Just . decodeRow . head) rows
            otherwise -> return Nothing
