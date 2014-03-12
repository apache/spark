import unittest

from boto import ec2
import mock
import moto

import spark_ec2


class CommandTests(unittest.TestCase):

    @moto.mock_ec2
    def test_destroy(self):
        spark_ec2.AWS_EVENTUAL_CONSISTENCY = 1
        opts = mock.MagicMock(name='opts')
        opts.region = "us-east-1"
        conn = ec2.connect_to_region(opts.region)
        cluster_name = "cluster_name"
        try:
            spark_ec2.destroy_cluster(conn, opts, cluster_name)
        except:
            self.fail("destroy_cluster raised unexpected exception")

if __name__ == '__main__':
    unittest.main()
