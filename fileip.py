def first():
    f = open("C:\\Users\\Nilay\\Desktop\\avro.avscdemofile2.txt", "a")
    f.write("Now the file has more content!{a}\nhiiiiiiiiiiiiiiiiiiiiiiiii\n".format(a="nilnilnil"))
    f.close()
    return f
def second(f):
    f = open("C:\\Users\\Nilay\\Desktop\\avro.avscdemofile2.txt", "a")
    f.write("Now the file has more content--------------------!\n")
    


if __name__ == "__main__":

    n=first()
    second(n)