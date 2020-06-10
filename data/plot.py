from absl import flags
from absl import app
from glob import glob
import matplotlib.pyplot as plt
import numpy as np

FLAGS = flags.FLAGS
flags.DEFINE_string('prefix', "green-pstack-ll", 'Name prefix of the measurments')

def plot_files(files):
    for file in files:
        x, y = np.loadtxt(file, comments='Threads' , unpack=True)

        with open(file) as f:
            first_line = f.readline().strip()
            print("line:")
            print(first_line)
            name = first_line.split()[1]
            print("name:")
            print(name)
        plt.plot(x, y, label=name)

    plt.xlabel('Threads')
    plt.ylabel('Ops/sec')
    plt.title('Comparison on 32 cores with 2 hyper-threads per core')
    plt.legend()
    plt.show()



def main(_):
    files = glob(FLAGS.prefix + "*")
    plot_files(files)


if __name__ == "__main__":
   app.run(main)

