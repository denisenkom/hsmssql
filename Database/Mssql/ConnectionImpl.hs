module Database.Mssql.ConnectionImpl where

import qualified Database.HDBC.Types as Types

data Connection = Connection {
    disconnect :: IO (),
    runRaw :: String -> IO (),
    prepare :: String -> IO Types.Statement
    }

instance Types.IConnection Connection where
    disconnect = disconnect
    runRaw = runRaw
    prepare = prepare
