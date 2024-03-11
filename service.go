package main

func initializeTransactionLog(logger TransactionLogger, store Store) error {
	var err error

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventPut:
				err = store.Put(e.Key, e.Value)
			case EventDelete:
				err = store.Delete(e.Key)
			}
		}
	}
	logger.Run()
	return err
}
