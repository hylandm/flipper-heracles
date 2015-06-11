import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class EventHandler( FileSystemEventHandler ):
    def on_any_event( self, event ):
        if not event.is_directory:
            print 'hello'
            print event

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    observer = Observer()
    observer.schedule(EventHandler(), path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
