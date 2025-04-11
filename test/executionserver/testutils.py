import os
import subprocess
import threading
import queue
import time


class TestExecutionServer:
    path: str
    process = None
    repl_output_queue = queue.Queue()
    ready = threading.Event()

    def __init__(self, path):
        self.path = path

    def start(self):
        if self.process is not None:
            print("Execution server already running")
            return

        cmd = "java -jar " + self.path + "/legend-engine-server-http-server-4.78.3-shaded.jar server " + self.path + "/userTestConfig.json"

        self.process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        print(os.getcwd())
        print(self.process)
        def wait_for_running():
            while self.process.poll() is None:
                line = self.process.stdout.readline()
                print(line)
                if line:
                    if "URL_FACTORY_REGISTERED" in line:
                        ## need to better read logs to determine if it has started
                        time.sleep(30)
                        self.ready.set()

        wait_thread = threading.Thread(target=wait_for_running, daemon=True)
        wait_thread.start()

        if not self.ready.wait(timeout=45):
            raise RuntimeError("Execution server did not respond")


    def stop(self):
        if self.process is not None:
            self.process.terminate()



def main():
    exec_server = TestExecutionServer(".")
    exec_server.start()
    print("Started!")
    time.sleep(60)
    exec_server.stop()

if __name__ == "__main__":
    main()