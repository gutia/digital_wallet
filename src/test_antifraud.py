from unittest import TestCase
from antifraud import Antifraud
from collections import namedtuple

input_batch_file = '../insight_testsuite/tests/test-2-paymo-trans/paymo_input/batch_payment.txt'
input_stream_file = '../insight_testsuite/tests/test-2-paymo-trans/paymo_input/stream_payment.txt'

class TestAntifraud(TestCase):
    def test_verified_connection_ft1(self):
        s = Antifraud()
        payments = s.parse_payments_file(input_batch_file)
        self.assertFalse(s.verified_connection_ft1(payments[0].id1, payments[0].id2))
        s.network_initialize(payments)
        self.assertTrue(s.verified_connection_ft1(payments[0].id1, payments[0].id2))

    def test_verified_connection_ft2(self):
        s = Antifraud()
        payments = s.parse_payments_file(input_batch_file)
        self.assertFalse(s.verified_connection_ft2(payments[0].id1, payments[0].id2))
        s.network_initialize(payments)
        verified1 = s.verified_connection_ft1(payments[0].id1, payments[0].id2)
        verified2 = s.verified_connection_ft2(payments[0].id1, payments[0].id2)
        self.assertTrue(verified1 or verified2)
        stream_payments = s.parse_payments_file(input_stream_file)
        self.assertTrue(s.verified_connection_ft2(stream_payments[2].id1, stream_payments[2].id2))

    def test_verified_connection_ft3(self):
        s = Antifraud()
        payments = s.parse_payments_file(input_batch_file)
        self.assertFalse(s.verified_connection_ft2(payments[0].id1, payments[0].id2))
        s.network_initialize(payments)
        stream_payments = s.parse_payments_file(input_stream_file)
        verified1 = s.verified_connection_ft1(stream_payments[3].id1, stream_payments[3].id2)
        verified2 = s.verified_connection_ft2(stream_payments[3].id1, stream_payments[3].id2)
        verified3 = s.verified_connection_ft3(stream_payments[3].id1, stream_payments[3].id2)
        self.assertTrue(verified1 or verified2 or verified3)


    def test_parse_payments_file(self):
        s = Antifraud()
        payments = s.parse_payments_file(input_batch_file)
        self.assertEqual(payments[0].id1, 'A')
        self.assertEqual(payments[0].id2, 'B')
        self.assertEqual(payments[0].amount, '5')
        self.assertEqual(len(payments), 5)

    def test_network_initialize(self):
        s = Antifraud()
        payments = s.parse_payments_file(input_batch_file)
        s.network_initialize(payments)
        self.assertEqual(len(s.users), 6)

    def test_network_stream_input(self):
        s = Antifraud()
        batch_payments = s.parse_payments_file(input_batch_file)
        s.network_initialize(batch_payments)
        stream_payments = s.parse_payments_file(input_stream_file)
        s.network_stream_input(stream_payments)
        self.assertEqual(len(s.users), 9)

