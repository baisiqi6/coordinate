import unittest

from coordinate.runner_examples import (
    get_runner_profile_example,
    list_runner_profile_examples,
)


class RunnerExamplesTests(unittest.TestCase):
    def test_list_examples_exposes_generic_subprocess_profiles(self):
        examples = list_runner_profile_examples()

        self.assertEqual([example["id"] for example in examples], ["codex-wrapper", "claude-wrapper"])
        self.assertEqual({example["runner_type"] for example in examples}, {"generic_subprocess"})

    def test_codex_example_uses_wrapper_and_agent_response_contract(self):
        example = get_runner_profile_example("codex-wrapper")
        profile = example["runner_profile"]

        self.assertEqual(profile["runner_type"], "generic_subprocess")
        self.assertEqual(profile["working_directory_strategy"], "git_worktree")
        self.assertIn("scripts/runners/codex-agent-response.sh", profile["command"])
        self.assertIn("{prompt_path}", profile["command"])
        self.assertIn("{result_path}", profile["command"])
        self.assertIn("summary", example["agent_response_fields"])
        self.assertIn("artifact_paths", example["agent_response_fields"])

    def test_unknown_example_raises_key_error(self):
        with self.assertRaisesRegex(KeyError, "unknown runner profile example"):
            get_runner_profile_example("missing")


if __name__ == "__main__":
    unittest.main()
