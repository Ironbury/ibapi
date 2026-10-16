package ibapi

import (
	"bufio"
	"context"
	"slices"
	"sync"
)

// EReader starts the scan and decode goroutines
func EReader(ctx context.Context, cancel context.CancelFunc, scanner *bufio.Scanner, decoder *EDecoder, wg *sync.WaitGroup) {

	msgChan := make(chan []byte, 300)

	// Decode
	wg.Add(1)
	go func() {
		log.Debug().Msg("decoder started")
		defer log.Debug().Msg("decoder ended")
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("decoder panic, triggering shutdown")
				cancel()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				decoder.parseAndProcessMsg(msg) // single worker and no go here!!
			}
		}
	}()

	// Scan
	wg.Add(1)
	go func() {
		log.Debug().Msg("scanner started")
		defer log.Debug().Msg("scanner ended")
		defer wg.Done()
		defer close(msgChan) // close the channel so decoder exits

		readMessages(ctx, cancel, scanner, msgChan)
	}()
}

// readMessages scans messages and queues them for the decoder until the
// input ends or fails (it then cancels the session) or the session ends.
func readMessages(ctx context.Context, cancel context.CancelFunc, scanner *bufio.Scanner, msgChan chan<- []byte) {
	for {
		select {
		case <-ctx.Done():
			// shutdown in flight
			return
		default:
			// block here until there's a token or an error/EOF
			if !scanner.Scan() {
				// only take action if we weren't already cancelled
				if ctx.Err() == nil {
					if err := scanner.Err(); err != nil {
						log.Error().Err(err).Msg("scanner error, triggering shutdown")
					} else {
						log.Debug().Msg("scanner reached EOF, triggering shutdown")
					}
					cancel()
				}
				return
			}

			// successful scan → queue for decode, unless the session
			// ends first: the decoder then reads no more, and a send
			// into a full queue would keep this goroutine, and so
			// Disconnect's wait, blocked for ever
			msgBytes := slices.Clone(scanner.Bytes())
			select {
			case msgChan <- msgBytes:
			case <-ctx.Done():
				return
			}
		}
	}
}
