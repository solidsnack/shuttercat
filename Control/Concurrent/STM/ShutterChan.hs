module Control.Concurrent.STM.ShutterChan where

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad
import           Data.Maybe


-- | Transfer all the contents of the first chan to the second as a block at
--   fixed intervals. The main thread handles timing and a new thread is
--   forked for each transfer operation.
--
--   When a 'Nothing' is present in the first channel, an empty list is
--   inserted in the second channel and the main thread terminates. The empty
--   list is a signal to consumers that the source is finished (the transfer
--   routine does not otherwise insert an empty list in the output channel).
--
--   Experimentally, tick rates of up to 40Hz are maintained stabley by this
--   routine.
--
--   The two 'IO' parameters allow one to schedule actions to be run before and
--   after each tick. (Set them to @return ()@ if no hooks are needed.)
transfer :: Int -> IO () -> IO () -> TChan (Maybe t) -> TChan [t] -> IO ()
transfer micros before after from to = do
  me <- myThreadId
  let go = forever (forkIO (step me) *> threadDelay micros)
  catchJust (guard . (== ThreadKilled)) go (const $ return ())
 where step main = do before
                      ended <- atomically (sendAll from to)
                      when ended (killThread main)
                      after

-- | Concatenate all the maybes in one channel and send them to the other as a
--   list, if it would not be empty. If there is a 'Nothing' in the first
--   channel, send an empty list on the second channel, to signal end of
--   input.
sendAll :: TChan (Maybe t) -> TChan [t] -> STM Bool
sendAll from to = do inputs   <- drain from
                     let items = catMaybes inputs
                         ended = any isNothing inputs
                     when (not (null items)) (writeTChan to items)
                     when ended              (writeTChan to [])
                     return ended

-- | Take all items from a channel.
drain :: TChan t -> STM [t]
drain chan = maybe (return []) recurse =<< tryReadTChan chan
 where recurse item = (item:) <$> drain chan

