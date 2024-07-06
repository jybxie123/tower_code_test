import os


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def acquire_pid_file(pid_file):
    if os.path.isfile(pid_file):
        with open(pid_file, 'r') as f:
            pid = int(f.read())
            if check_pid(pid):
                print("Another instance is already running.")
                return False
            else:
                os.remove(pid_file)
    
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))
    return True

def release_pid_file(pid_file):
    if os.path.isfile(pid_file):
        os.remove(pid_file)
