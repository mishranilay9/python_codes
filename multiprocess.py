import concurrent.futures
import time

def do_something(a):
    print("sleeping for {} seconds !!..\n".format(a))
    time.sleep(a)
    return "done sleeping {} seconds !!..\n".format(a)

if __name__ == "__main__":
    start=time.perf_counter()

    #For Single process
    with concurrent.futures.ProcessPoolExecutor() as executor:
        f1=executor.submit(do_something,1)
    print(f1.result())
    #-------------------------------------------------------------------------------------------
    #To run in loop
    secs=[5,4,3,2,1]
    with concurrent.futures.ProcessPoolExecutor() as executor1:
        fs = [executor1.submit(do_something, sec) for sec in secs]
    for f in concurrent.futures.as_completed(fs):
        print(f.result())
    #--------------------------------------------------------------------------------------------
    #using python map
    with concurrent.futures.ProcessPoolExecutor() as executor2:
        results = executor2.map(do_something,secs)
    for result in results:
        print(result)

    end = time.perf_counter()
    print("Execution time is {}".format(end-start))



