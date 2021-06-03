from random import shuffle

full_size = 100000000

integers = [str(x) for x in range(full_size)]
shuffle(integers)

for i in range(1,11):
    size = full_size / 10 * i
    with open(f'{i}.csv', 'w+') as f:
        print('\n'.join(integers[:size]), file=f)

