from absl import flags
from absl import app
from glob import glob
import matplotlib.pyplot as plt
import numpy as np

FLAGS = flags.FLAGS
flags.DEFINE_string('prefix', "green-pstack-ll", 'Name prefix of the measurements')

name_to_index = {"DFC": 0, "RomLog-FC": 1, "OneFilePTM-LF": 2, "PMDK": 3}


def plot_files(files):
    data = [None] * 4
    for file in files:
        x, y = np.loadtxt(file, comments='Threads', unpack=True)
        to40 = x <= 40
        indices = np.where(np.in1d(x, [1,4,8,16,24,32,40]))[0]
        x = x[indices]
        y = y[indices]

        with open(file) as f:
            first_line = f.readline().strip()
            name = first_line.split()[1]
            description = name.rfind("-")
            if description != -1:
                name = name[:description]

        data[name_to_index[name]] = [x, y, name]

    for d in data:
        plt.plot(d[0], d[1], 'o-', label=d[2])

    plt.xlabel('Threads', size=10)
    plt.ylabel('Ops/sec', size=10)
    plt.yticks([1e5, 3e5, 5e5, 7e5, 9e5, 11e5, 13e5, 15e5], (r'$1\times 10^5$', r'$3\times 10^5$', r'$5\times 10^5$',
                                                             r'$7\times 10^5$', r'$9\times 10^5$', r'$1.1\times 10^6$',
                                                             r'$1.3\times 10^6$', r'$1.5\times 10^6$'))
    # plt.xticks([1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40], (1, 2, 4, 8, 10, 16, 18, 20,
    #                                                                               22, 24, 26, 28, 30, 32, 34,
    #                                                                               36, 40))
    plt.xticks([1, 4, 8, 16, 24, 32, 40], (1, 4, 8, 16, 24, 32, 40))
        # plt.title('Comparison on 32 cores on nvram')
    # plt.title('Comparison on 40 logical cores machine\nwith NVRAM, using CLFLUSHOPT')
    plt.legend()
    plt.tight_layout()
    plt.savefig("Graph_of_" + FLAGS.prefix, dpi=None)
    plt.show()


def plot_pwb_files(files):
    data = [None] * 3
    for file in files:
        try:
            x, y1, y2, y3, y4 = np.loadtxt(file, comments='Threads', unpack=True)
            to40 = x <= 40
            indices = np.where(np.in1d(x, [1,4,8,16,24,32,40]))[0]
            x = x[indices]
            y1 = y1[indices]
            y2 = y2[indices]
            y3 = y3[indices]
            y4 = y4[indices]
            with open(file) as f:
                first_line = f.readline().strip()
                name = first_line.split()[1]
                description = name.rfind("-Linked")
                if description != -1:
                    name = name[:description]

                description = name.rfind("-P")
                if description != -1:
                    name = name[:description]

            data[name_to_index[name]] = [x, y1, y2, name, y3, y4]

        except ValueError:
            x, y1, y2 = np.loadtxt(file, comments='Threads', unpack=True)
            to40 = x <= 40
            indices = np.where(np.in1d(x, [1,4,8,16,24,32,40]))[0]
            x = x[indices]
            y1 = y1[indices]
            y2 = y2[indices]

            with open(file) as f:
                first_line = f.readline().strip()
                name = first_line.split()[1]
                description = name.rfind("-Linked")
                if description != -1:
                    name = name[:description]

                description = name.rfind("-P")
                if description != -1:
                    name = name[:description]

            data[name_to_index[name]] = [x, y1, y2, name]

    for d in data:
        plt.plot(d[0], d[1], 'o-', label=d[3])
    plt.plot(data[0][0], data[0][4], 'o:', label="Total " + data[0][3])

    plt.xlabel('Threads', size=10)
    plt.ylabel('PWB/Op', size=10)
    # plt.xticks([1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40], (1, 2, 4, 8, 10, 16, 18, 20,
    #                                                                               22, 24, 26, 28, 30, 32, 34,
    #                                                                               36, 40))
    plt.xticks([1, 4, 8, 16, 24, 32, 40], (1, 4, 8, 16, 24, 32, 40))
    plt.yticks([0, 1.5, 3.5, 8.5, 20, 30, 40, 50], (0, 1.5, 3.5, 8.5, 20, 30, 40, 50))
    # plt.title('Comparison of PWB per Operation')
    plt.legend()
    plt.savefig("PWB_Graph_of_" + FLAGS.prefix, dpi=None)
    plt.show()
    plt.close()

    for d in data:
        plt.plot(d[0], d[2], 'o-', label=d[3])
    plt.plot(data[0][0], data[0][5], 'o:', label="Total "+data[0][3])

    plt.xlabel('Threads', size=10)
    plt.ylabel('PFENCE/Op', size=10)
    # plt.xticks([1, 2, 4, 8, 10, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 40], (1, 2, 4, 8, 10, 16, 18, 20,
    #                                                                               22, 24, 26, 28, 30, 32, 34,
    #                                                                               36, 40))
    plt.xticks([1, 4, 8, 16, 24, 32, 40], (1, 4, 8, 16, 24, 32, 40))
    plt.yticks(size=10)
    # plt.title('Comparison of PFENCE per Operation')
    plt.legend()
    plt.savefig("PFENCE_Graph_of_" + FLAGS.prefix, dpi=None)
    plt.show()


def main(_):
    files = glob(FLAGS.prefix + "*")
    if FLAGS.prefix.startswith("pwb-pfence"):
        plot_pwb_files(files)
    else:
        plot_files(files)


if __name__ == "__main__":
   app.run(main)

