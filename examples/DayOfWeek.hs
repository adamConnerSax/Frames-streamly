{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module DayOfWeek where

import qualified Frames.Streamly.ColumnTypeable as FStreamly
import Frames.Streamly.OrMissing
import qualified Data.Readable as Readable
import Frames (Readable(fromText))
--import qualified Frames as Readble


data DayOfWeek = Monday | Tuesday | Wednesday | Thursday | Friday | Saturday | Sunday deriving (Show, Eq, Enum, Bounded)


-- This derives Unbox and VectorFor for DayOfWeek and (OrMissing DayOfWeek)
derivingOrMissingUnboxVectorFor'
  (derivingUnbox
    "DayOfWeek"
    [t|DayOfWeek -> Word8|]
    [e|fromIntegral . fromEnum|]
    [e|toEnum . fromIntegral|])
  "DayOfWeek"
  [e|Monday|]

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
  parse = fmap FStreamly.Definitely . Readable.fromText
