module Database.Mssql.Statement where
import Database.Mssql.Tds

import Control.Concurrent.MVar
import Database.HDBC
import Database.HDBC.Types
import System.IO

data SState =
    SState { conn :: Handle,
             squery :: String,
             coldefmv :: MVar [(String, SqlColDesc)] }

newSth :: Handle -> String -> IO Statement
newSth conn query =
    do newcoldefmv <- newMVar []
       let sstate = SState {conn = conn, squery = query,
                            coldefmv = newcoldefmv}
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
       return ()


fgetColumnNames :: SState -> IO [(String)]
fgetColumnNames sstate =
    do c <- readMVar (coldefmv sstate)
       return (map fst c)


ffetchRow :: SState -> IO (Maybe [SqlValue])
ffetchRow sstate =
    do return Nothing
