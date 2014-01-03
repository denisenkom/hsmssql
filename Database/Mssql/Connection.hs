module Database.Mssql.Connection where

import qualified Database.HDBC.Types as Types

data Connection = Connection {
    disconnect :: IO (),
    runRaw :: String -> IO ()
    }

instance Types.IConnection Connection where
    disconnect = disconnect
    runRaw = runRaw
