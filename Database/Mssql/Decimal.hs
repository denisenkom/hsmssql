module Database.Mssql.Decimal where

import Control.Monad
import qualified Data.Binary.Get as LG
import qualified Data.Binary.Put as LP
import Data.Bits
import Data.Int
import Data.Ratio
import Data.Word


data Decimal = Decimal {prec :: Word8,
                        scale :: Word8,
                        val :: Integer}
    deriving (Eq, Ord)

instance Num Decimal where
    abs (Decimal prec scale val) = Decimal prec scale (abs val)
    signum (Decimal prec scale val) = Decimal prec scale (signum val)
    fromInteger val = Decimal 38 0 val
    (+) (Decimal prec1 scale1 val1) (Decimal prec2 scale2 val2) = Decimal (max prec1 prec2) (max scale1 scale2) (val1 + val2)

instance Real Decimal where
    toRational (Decimal prec scale val) = val % (10 ^ scale)

instance Show Decimal where
    show dec = show $ toRational dec


decimalFold :: [Word32] -> Integer
decimalFold = foldr (\v acc -> (fromIntegral v) + (acc `shiftL` 32)) 0

decimalSize :: Word8 -> Int
decimalSize prec
    | 1 <= prec && prec <= 9 = 5
    | 10 <= prec && prec <= 19 = 9
    | 20 <= prec && prec <= 28 = 13
    | 29 <= prec && prec <= 38 = 17

getDecimal :: Word8 -> Word8 -> Int -> LG.Get Decimal
getDecimal prec scale size = do
    sign <- LG.getWord8
    ints <- replicateM ((size - 1) `quot` 4) LG.getWord32le
    let val = (decimalFold ints)
        res = if sign == 0 then (-val) else val
    return $ Decimal prec scale res


putDecimal :: Word8 -> Word8 -> Decimal -> LP.Put
putDecimal tprec tscale (Decimal prec scale val) = do
    if tprec /= prec || tscale /= scale
        then fail "TODO: cast decimal to type's precision and scale"
        else return ()
    let sign = if val >= 0 then 1 else 0
        aval = abs val
        size = decimalSize tprec
        numInts = (size - 1) `quot` 4
        putInt 0 0 = return ()
        putInt i val = do
            let (q, r) = quotRem val 0x100000000
            LP.putWord32le $ fromIntegral r
            putInt (i - 1) q
    LP.putWord8 sign
    putInt numInts aval
