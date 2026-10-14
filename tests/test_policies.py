from unittest import TestCase

from aerospike_native import ReadPolicy, RecordExistsAction, WritePolicy

from ddaerospike.policies import DEFAULT_READ_POLICY, DEFAULT_WRITE_POLICY, build_policy


class TestBuildPolicy(TestCase):
    def test_sets_the_given_fields_and_keeps_the_rest(self):
        # Act
        policy = build_policy(ReadPolicy, DEFAULT_READ_POLICY)

        # Assert
        self.assertIsInstance(policy, ReadPolicy)
        self.assertEqual(policy.total_timeout, 2_000)
        self.assertEqual(policy.max_retries, 2)
        self.assertEqual(policy.sleep_between_retries, 10)

    def test_overrides_merge_over_the_defaults(self):
        # Act
        policy = build_policy(WritePolicy, {**DEFAULT_WRITE_POLICY, 'record_exists_action': RecordExistsAction.CREATE_ONLY})

        # Assert
        self.assertTrue(policy.send_key)
        self.assertEqual(policy.max_retries, 0)
        self.assertEqual(policy.record_exists_action, RecordExistsAction.CREATE_ONLY)
