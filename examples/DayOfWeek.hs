{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module DayOfWeek where

import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Streamly.ColumnTypeable as FStreamly
import qualified Data.Readable as Readable
import Data.Vector.Unboxed.Deriving
import qualified Data.Vector.Unboxed as UVec
import Language.Haskell.TH.Syntax (Lift)
import Streamly.Internal.Foreign.Malloc (mallocForeignPtrAlignedUnmanagedBytes)
import Frames (Readable(fromText))
import qualified Frames as Readble


data DayOfWeek = Monday | Tuesday | Wednesday | Thursday | Friday | Saturday | Sunday deriving (Show, Eq, Enum, Bounded, Lift)

derivingUnbox
  "DayOfWeek"
  [t|DayOfWeek -> Word8|]
  [e|fromIntegral . fromEnum|]
  [e|toEnum . fromIntegral|]

type instance FStreamly.VectorFor DayOfWeek = UVec.Vector

instance Readable.Readable DayOfWeek where
  fromText "mon" = return Monday
  fromText "tue" = return Tuesday
  fromText "wed" = return Wednesday
  fromText "thu" = return Thursday
  fromText "fri" = return Friday
  fromText "sat" = return Saturday
  fromText "sun" = return Sunday
  fromText _ = mzero

instance FStreamly.Parseable DayOfWeek where
  parse = fmap FStreamly.Definitely . Readble.fromText
