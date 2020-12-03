def add(mat_A, mat_B):
    mu = []
    for (i, j) in zip(mat_A, mat_B):
        su = []
        for (a, b) in zip(i, j):
            su.append(a + b)
        mu.append(su)
    return mu

if __name__ == "__main__":
    mat_A = [
        [1, 4, 5, 12],
        [-5, 8, 9, 0],
        [-6, 7, 11, 19]
    ]
    mat_B = [
        [1, 4, 5, 13],
        [-5, 8, 9, 0],
        [-6, 7, 11, 19]
    ]
    #s = add(mat_A, mat_B)
    coordinate = ['x', 'y', 'z']
    value = [3, 4, 5]

    result = zip(coordinate, value)
    result_list = list(result)
    print(result_list)
    c, v = zip(*result_list)
    print('c =', c)
    print('v =', v)


