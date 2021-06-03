from random import shuffle

full_size = 100000000
integers = [str(x) for x in range(full_size)]

with open('100asc.csv', 'w+') as f:
    print('\n'.join(integers), file=f)

with open('100desc.csv', 'w+') as f:
    print('\n'.join(integers[::-1]), file=f)

shuffle(integers)

for i in range(1,11):
    size = int(full_size / 10) * i
    with open(f'data/{i*10}.csv', 'w+') as f:
        print('\n'.join(integers[:size]), file=f)

