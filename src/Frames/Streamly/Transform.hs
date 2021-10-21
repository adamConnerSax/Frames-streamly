{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-|
Module      : Frames.Streamly.Transform
Description : Support for transformations on Frames using streamly streams as an intermediate structure.
Copyright   : (c) Adam Conner-Sax 2020
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

This module adds some functions for using streamly to make transformations on Frames that are either:

1. Like filtering or some one-to-many mapping of rows,
whic makes the efficient memory layout useless in-flight.  Frames already implements 'filterFrame'
this way, using Pipes and the ST monad.

2. Expensive enough per row that the concurrency features of streams become worthwhile.
-}
module Frames.Streamly.Transform
    ( transform
    , filter
    , concurrentMapM
    , mapMaybe
    , concurrentMapMaybeM
    )
where

import qualified Frames.Streamly.InCore as FS
import Frames.Streamly.Internal.Streamly (streamlyFunctions)
import Prelude hiding (filter, mapMaybe)

#if MIN_VERSION_streamly(0,8,0)
import Streamly.Prelude                       (IsStream)
#else
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
#endif

import qualified Streamly.Prelude                       as Streamly
import qualified Control.Monad.Primitive                as Prim
import Control.Monad.ST (runST)
import qualified Frames
import qualified Frames.InCore                          as Frames

#if MIN_VERSION_streamly(0,8,0)
fromSerial :: IsStream t => Streamly.SerialT m a -> t m a
fromSerial = Streamly.fromSerial
{-# INLINE fromSerial #-}

fromAhead :: IsStream t => Streamly.AheadT m a -> t m a
fromAhead = Streamly.fromAhead
{-# INLINE fromAhead #-}
#else
fromSerial :: IsStream t => Streamly.SerialT m a -> t m a
fromSerial = Streamly.serially
{-# INLINE fromSerial #-}

fromAhead :: IsStream t => Streamly.AheadT m a -> t m a
fromAhead = Streamly.aheadly
{-# INLINE fromAhead #-}
#endif


-- | Use streamly to transform a frame.
transform ::
  forall t as bs m.
  (IsStream t
  , Streamly.MonadAsync m
  , Prim.PrimMonad m
  , Frames.RecVec as
  , Frames.RecVec bs
  )
  => (t m (Frames.Record as) -> Streamly.SerialT m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
transform f = FS.inCoreAoS streamlyFunctions . f . Streamly.fromFoldable
{-# INLINE transform #-}

-- | Filter using streamly
filter :: (Frames.RecVec as) => (Frames.Record as -> Bool) -> Frames.FrameRec as -> Frames.FrameRec as
filter f frame = runST $ transform (fromSerial . Streamly.filter f) frame
{-# INLINE filter #-}

-- | map using speculative streams (concurrency that preserves ordering of results).
concurrentMapM :: (Prim.PrimMonad m
                  , Streamly.MonadAsync m
                  , Frames.RecVec as
                  , Frames.RecVec bs
                  ) => (Frames.Record as -> m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapM f = transform (fromAhead . Streamly.mapM f)
{-# INLINE concurrentMapM #-}

-- | mapMaybe using streamly
mapMaybe :: (Frames.RecVec as
                      , Frames.RecVec bs
                      ) => (Frames.Record as -> Maybe (Frames.Record bs)) -> Frames.FrameRec as -> Frames.FrameRec bs
mapMaybe f frame = runST $ transform (fromAhead . Streamly.mapMaybe f) frame
{-# INLINE mapMaybe #-}

-- | mapMaybeM using speculative streams (concurrency that preserves ordering of results).
concurrentMapMaybeM :: (Prim.PrimMonad m
                       , Streamly.MonadAsync m
                       , Frames.RecVec as
                       , Frames.RecVec bs
                       ) => (Frames.Record as -> m (Maybe (Frames.Record bs))) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapMaybeM f = transform (fromAhead . Streamly.mapMaybeM f)
{-# INLINE concurrentMapMaybeM #-}
