module Database.Mssql.Statement where
import Database.Mssql.Tds
import Database.HDBC
import Database.HDBC.Types
import System.IO

data SState =
    SState { conn :: Handle,
             squery :: String}

newSth :: Handle -> String -> IO Statement
newSth conn query =
    do let sstate = SState {conn = conn, squery = query}
           retval = Statement {executeRaw = fexecuteRaw sstate}
       return retval


fexecuteRaw :: SState -> IO ()
fexecuteRaw sstate =
    do tokens <- exec (conn sstate) (squery sstate)
       print tokens
       return ()
