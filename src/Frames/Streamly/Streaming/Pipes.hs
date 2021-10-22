{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
module Frames.Streamly.Streaming.Pipes
  (
    pipesFunctions
  , pipesFunctionsIO
  , pipesFunctionsWithIO
  , PipeStream
  ) where

import Frames.Streamly.Streaming.Interface

import Frames.Streamly.Internal.CSV (FramesCSVException(..))

import qualified Pipes
import Pipes ((>->))
import qualified Pipes.Prelude as Pipes
import qualified System.IO as IO

import qualified Control.Foldl as Foldl
import           Control.Monad.Catch                     ( MonadThrow(..))
--import Control.Monad.IO.Class (MonadIO(..))

import qualified Data.Text as T

newtype PipeStream m a = PipeStream { producer :: Pipes.Producer a m () }

type instance FoldType PipeStream = Foldl.FoldM

pipesFunctions :: (MonadThrow m, Monad m) => StreamFunctions PipeStream m
pipesFunctions = StreamFunctions
  (pipesThrowIfEmpty . producer)
  (\a s -> PipeStream $ Pipes.yield a >> producer s)
  pipeStreamUncons
  (Pipes.head . producer)
  (\f s -> PipeStream $ producer s >-> Pipes.map f)
  (\f s -> PipeStream $  producer s >-> Pipes.wither (return . f))
  (\step start s -> PipeStream $ producer s >-> Pipes.scanM step start return)
  (\n s -> PipeStream $ producer s >-> Pipes.drop n)
  (\n s -> PipeStream $ producer s >-> Pipes.take n)
  (\step start -> pipesFolder step start . producer)
  pipesBuildFold
  pipesBuildFoldM
  pipesPostMapM
  (\fld s -> Foldl.impurely Pipes.foldM fld $ producer s)
  (Pipes.toListM . producer) -- this might be bad (not lazy) compared to streamly.
  (PipeStream . pipesFromFoldable)
{-# INLINEABLE pipesFunctions #-}

pipesPostMapM :: Monad m => (b -> m c) -> Foldl.FoldM m a b -> Foldl.FoldM m a c
pipesPostMapM f (Foldl.FoldM step begin done) = Foldl.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE pipesPostMapM #-}

pipesFunctionsIO :: (Monad m, MonadThrow m, MonadIO m) => StreamFunctionsIO PipeStream m
pipesFunctionsIO = StreamFunctionsIO
  (PipeStream . pipesReadTextLines)
  (\fp -> pipesWriteTextLines fp . producer)
{-# INLINEABLE pipesFunctionsIO #-}

pipesFunctionsWithIO :: (Monad m, MonadThrow m, MonadIO m) => StreamFunctionsWithIO PipeStream m
pipesFunctionsWithIO = StreamFunctionsWithIO pipesFunctions pipesFunctionsIO
{-# INLINEABLE pipesFunctionsWithIO #-}

pipesBuildFold :: Monad m => (x -> a -> x) -> x -> (x -> b) -> Foldl.FoldM m a b
pipesBuildFold step start extract = Foldl.generalize $ Foldl.Fold step start extract
{-# INLINE pipesBuildFold #-}

pipesBuildFoldM :: (x -> a -> m x) -> m x -> (x -> m b) -> Foldl.FoldM m a b
pipesBuildFoldM = Foldl.FoldM
{-# INLINE pipesBuildFoldM #-}

pipesThrowIfEmpty :: MonadThrow m => Pipes.Producer a m () -> m ()
pipesThrowIfEmpty s = Pipes.null s >>= \b -> if b then throwM EmptyStreamException else return ()
{-# INLINE pipesThrowIfEmpty #-}

pipesFolder :: Monad m => (x -> b -> x) -> x -> Pipes.Producer b m () -> m x
pipesFolder step start = Pipes.fold step start id
{-# INLINE pipesFolder #-}


pipesFromFoldable :: (Functor m, Foldable f) => f a -> Pipes.Producer a m ()
pipesFromFoldable = Pipes.each
{-# INLINE pipesFromFoldable #-}

-- how/when does this handle get closed??
pipesReadTextLines :: MonadIO m => FilePath -> Pipes.Producer Text m ()
pipesReadTextLines fp = do
  h <- Pipes.lift $ liftIO $ IO.openFile fp IO.ReadMode
  Pipes.fromHandle h Pipes.>-> Pipes.map T.pack
{-# INLINABLE pipesReadTextLines #-}

pipesWriteTextLines :: MonadIO m => FilePath -> Pipes.Producer Text m () -> m ()
pipesWriteTextLines fp s = do
  h <- liftIO $ IO.openFile fp IO.WriteMode
  Pipes.runEffect $ s Pipes.>-> Pipes.map T.unpack Pipes.>-> Pipes.toHandle h
  liftIO $ IO.hClose h
{-# INLINABLE pipesWriteTextLines #-}

pipeStreamUncons :: Monad m => PipeStream m a -> m (Maybe (a, PipeStream m a))
pipeStreamUncons p = do
  pUncons <- Pipes.next (producer p)
  case pUncons of
    Left () -> return Nothing
    Right (a, s) -> return $ Just (a, PipeStream s)
{-# INLINABLE pipeStreamUncons #-}
