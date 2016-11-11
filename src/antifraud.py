import sys
import re
from collections import namedtuple


class Antifraud:

    def __init__(self):
        self.users = {}
        self.output_ft1 = []
        self.output_ft2 = []
        self.output_ft3 = []

    def verified_connection_ft1(self, id1, id2):
        """ verified_connection_ft1(string, string) -> boolean
        Feature 1: friends
        """
        if id1 in self.users.keys():
            return id2 in self.users[id1]
        return False

    def verified_connection_ft2(self, id1, id2):
        """ verified_connection_ft1(string, string) -> boolean
        Feature 2: friend in common
        """
        if id1 in self.users.keys():
            return id2 in self.users.keys() and len(self.users[id1] & self.users[id2]) != 0
        return False

    def verified_connection_ft3(self, id1, id2):
        """ verified_connection_ft3(string, string) -> boolean
        Feature 3: have 3rd or 4th degree connection (friend of a friend)
        """
        if id2 not in self.users.keys() or id1 not in self.users.keys():
            return False

        f1 = self.users[id1]
        f2 = self.users[id2]
        fof1 = self.friends_of_friends(id1, f1)
        fof2 = self.friends_of_friends(id2, f2)
        return len((f1 | fof1) & (f2 | fof2)) != 0


    def friends_of_friends(self, node, ids):
        """ friends_of_friends(list of strings) -> set of strings
        Returns set of all friends for given ids
        """
        fof = set()
        for id in ids:
            for f in self.users[id]:
                if f != node:
                    fof.add(f)
        return fof

    def parse_payments_file(self, fname):
        """ parse_payments_file(path to file) -> list of named tuples
        """
        f = open(fname, 'r')
        fields = " ".join(f.readline().split())
        content = f.read()
        f.close()

        payment = namedtuple('payment', fields)
        payments = []
        split_content = re.split(r'\s+(?=\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},)', content)

        for line in split_content:
            ln = line.strip().rsplit(', ')
            if len(ln) > 5:
                ln[4] = ', '.join(ln[4:])
                ln = ln[0:5]
            pt = payment(*ln)
            payments.append(pt)

        return payments

    def network_initialize(self, payments):
        """ network_initialize(list of named tuples with user ids) -> None
        Initalizes the network with users based on input list of payments.
        """
        for pt in payments:
            if pt.id1 not in self.users.keys():
                self.users[pt.id1] = set()
            if pt.id2 not in self.users.keys():
                self.users[pt.id2] = set()
            self.users[pt.id1].add(pt.id2)
            self.users[pt.id2].add(pt.id1)


    def network_stream_input(self, payments):
        """ network_stream_input(list of named tuples with user ids) -> None
        Verifies each transaction based on three features and prints results to files
        """
        for pt in payments:

            if pt.id1 not in self.users.keys():
                self.users[pt.id1] = set()

            if pt.id2 not in self.users.keys():
                self.users[pt.id2] = set()

            verified_ft1 = self.verified_connection_ft1(pt.id1, pt.id2)
            if verified_ft1:
                self.output_ft1.append("trusted")
            else:
                self.output_ft1.append("unverified")

            verified_ft2 = self.verified_connection_ft2(pt.id1, pt.id2)
            if verified_ft1 or verified_ft2:
                self.output_ft2.append("trusted")
            else:
                self.output_ft2.append("unverified")

            verified_ft3 = self.verified_connection_ft3(pt.id1, pt.id2)
            if verified_ft3 or verified_ft1 or verified_ft2:
                self.output_ft3.append("trusted")
            else:
                self.output_ft3.append("unverified")

            self.users[pt.id1].add(pt.id2)
            self.users[pt.id2].add(pt.id1)

    def write_results(self, fname, results):
        with open(fname, "w") as f:
            for res in results:
                f.write("{}\n".format(res))

def main():
    batch_payment_file = sys.argv[1]
    stream_payment_file = sys.argv[2]
    output1_file = sys.argv[3]
    output2_file = sys.argv[4]
    output3_file = sys.argv[5]

    antifraud = Antifraud()

    print("Reading payments to initialize network...")
    transactions = antifraud.parse_payments_file(batch_payment_file)
    print("Read {} transactions".format(len(transactions)))
    print("Initializing network...")
    antifraud.network_initialize(transactions)
    print("Done initializing network with {} users".format(len(antifraud.users)))
    print("Reading stream payments to add to the network...")
    transactions = antifraud.parse_payments_file(stream_payment_file)
    print("Read {} transactions to be added to the network...".format(len(transactions)))
    antifraud.network_stream_input(transactions)
    print("Resulting network contains {} users".format(len(antifraud.users)))
    antifraud.write_results(output1_file, antifraud.output_ft1)
    antifraud.write_results(output2_file, antifraud.output_ft2)
    antifraud.write_results(output3_file, antifraud.output_ft3)
    print("Output is written to files")
    print("Done.")



if __name__ == "__main__":
    main()








