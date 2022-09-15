import random
import time
from pyJoules.energy_meter import measure_energy
from pyJoules.handler import EnergyHandler
import psutil
import GPUtil

import measure
import os.path

process_pool = []
undecorated_pool = []


def _gen_sample_line(sample, domain_names):
    line_beginning = f'{sample.timestamp};{sample.tag};{sample.duration};'
    energy_values = [str(sample.energy[domain]) for domain in domain_names]
    return line_beginning + ';'.join(energy_values)


def _gen_header(first_sample):
    domain_names = first_sample.energy.keys()
    return 'timestamp;tag;duration;' + ';'.join(domain_names)


class CSVHandler(EnergyHandler):

    def __init__(self, filename: str):
        """
        :param filename: file name to store processed trace
        """
        EnergyHandler.__init__(self)

        self._filename = filename

    def _init_file(self, first_sample):
        if os.path.exists(self._filename):
            csv_file = open(self._filename, 'a+')
            return csv_file
        else:
            csv_file = open(self._filename, 'w+')
            csv_file.write(_gen_header(first_sample) + '\n')
            return csv_file

    def save_data(self):
        """
        append processed trace to the file
        """
        flattened_trace = self._flaten_trace()
        first_sample = flattened_trace[0]
        domain_names = first_sample.energy.keys()

        csv_file = self._init_file(first_sample)

        for sample in flattened_trace:
            csv_file.write(_gen_sample_line(sample, domain_names) + '\n')
        csv_file.close()
        self.traces = []


def decorated_method(func, *decorators):
    for d in reversed(decorators):
        for iteration in range(n_iterations):
            func = d(func)
            process_pool.append(func)
        for w_it in range(warmup):
            undecorated_pool.append(func)


def build_pool(subject):
    process_pool.clear()
    for name in dir(subject):
        method = getattr(subject, name)
        if callable(method) and name.startswith("test_"):
            csv_handler = CSVHandler(f"{name}.csv")
            decorated_method(method,
                             measure_energy(handler=csv_handler)
                             )


if __name__ == '__main__':
    warmup = 10
    n_iterations = 30
    sleep_time = 60
    shuffle_pool = True
    build_pool(
        measure
    )
    if shuffle_pool:
        random.shuffle(undecorated_pool)
        random.shuffle(process_pool)
    for proc in undecorated_pool:
        proc()
    for proc in process_pool:
        proc()
        psutil.sensors_temperatures()  # {...}
        print('The CPU usage over 4 s is: ', psutil.cpu_percent(4))
        print('RAM memory % used:', psutil.virtual_memory()[2])
        GPUtil.showUtilization()
        time.sleep(sleep_time)