module Database.Mssql.Decimal where

import Control.Monad
import qualified Data.Binary.Get as LG
import qualified Data.Binary.Put as LP
import Data.Bits
import Data.Decimal
import Data.Int
import Data.Ratio
import Data.Word


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
    return $ Decimal scale res


putDecimal :: Word8 -> Word8 -> Decimal -> LP.Put
putDecimal tprec tscale (Decimal scale val) = do
    if tscale /= scale
        then fail "TODO: cast decimal to type's precision and scale"
        else return ()
    let sign = if val >= 0 then 1 else 0
        aval = abs val
        size = decimalSize tprec
        numInts = (size - 1) `quot` 4
        putInt 0 0 = return ()
        putInt 0 val = fail $ "decimal is not fully consumed: " ++ show val
        putInt i val = do
            let (q, r) = quotRem val 0x100000000
            LP.putWord32le $ fromIntegral r
            putInt (i - 1) q
    LP.putWord8 sign
    putInt numInts aval

rationalToDec :: Rational -> Decimal
rationalToDec val =
    let mdec = eitherFromRational val
    in case mdec of
        Right dec -> dec
        Left msg -> error msg

rationalScale :: Rational -> Word8
rationalScale = decimalPlaces . rationalToDec
