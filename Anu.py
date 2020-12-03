def find_Arms(num):
    sum=0
    temp = num
    while temp > 0:
        digit = temp % 10
        sum += digit ** 3
        temp //= 10
        #same thing as
        # r = n % 10;
        # sum = sum + (r * r * r);
        # n = n / 10;
    if num == sum:
        return 1
    else:
        return 0
if __name__ == "__main__":
    #Take your input parameter
    n=100
    #in c just put a for loop
    #for(i=1;i<n;i++)
    for  i in range(1,n+1,1):
        n=find_Arms(i)
        if (n==1):
            print("{} is an armstorng number".format(i))
        else:
            pass



