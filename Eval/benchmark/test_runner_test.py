import unittest

from test_runner import compare_plan_with_oracle


class TestComparePlanWithOracle(unittest.TestCase):
    def test_exact_path_match(self):
        tasks = [
            {"operation": "GET", "url": "http://localhost/Spotify/1.0/tracks/123"}
        ]
        oracle = ["GET /Spotify/1.0/tracks/123"]

        result = compare_plan_with_oracle(tasks, oracle)
        self.assertTrue(result["is_correct_path"])
        self.assertEqual(result["num_planned"], 1)
        self.assertEqual(result["num_oracle"], 1)
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["planned"][1], "/Spotify/1.0/tracks/123")

    def test_double_braced_planned_placeholder_matches_oracle_placeholder(self):
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/tracks/{{get_user.id}}"}
        ]
        oracle = ["GET /rest/Spotify/1.0/tracks/{id}"]

        result = compare_plan_with_oracle(tasks, oracle)
        self.assertTrue(result["is_correct_path"])
        self.assertEqual(len(result["matches"]), 1)

    def test_service_prefix_suffix_match(self):
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/artist/{{get_artist.id}}/albums"}
        ]
        oracle = ["GET /artist/{id}/albums"]

        result = compare_plan_with_oracle(tasks, oracle)
        self.assertTrue(result["is_correct_path"])
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["oracle"][1], "/artist/{id}/albums")

    def test_mismatch_when_paths_differ(self):
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/tracks/123"}
        ]
        oracle = ["GET /albums/{id}"]

        result = compare_plan_with_oracle(tasks, oracle)
        self.assertFalse(result["is_correct_path"])
        self.assertEqual(len(result["matches"]), 0)
        self.assertEqual(len(result["mismatches"]), 1)
        self.assertEqual(len(result["oracle_missed"]), 1)

    # ---- New tests: canonical RestBench subsequence semantics ----

    def test_extra_steps_do_not_break_correct_path(self):
        """Gold appears as a subsequence; an extra planned call must NOT fail CP."""
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/search"},
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/me"},
            # extra, not in the gold path:
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/artists/{{x.id}}/top-tracks"},
            {"operation": "POST", "url": "http://localhost/rest/Spotify/1.0/users/me/playlists"},
            {"operation": "POST", "url": "http://localhost/rest/Spotify/1.0/playlists/{{p.id}}/tracks"},
        ]
        oracle = [
            "GET /search",
            "GET /me",
            "POST /users/{user_id}/playlists",
            "POST /playlists/{playlist_id}/tracks",
        ]
        result = compare_plan_with_oracle(tasks, oracle)
        self.assertTrue(result["is_correct_path"])          # extra step allowed
        self.assertEqual(len(result["oracle_missed"]), 0)
        self.assertEqual(len(result["mismatches"]), 1)       # the top-tracks call
        self.assertEqual(result["step_recall"], 1.0)         # all 4 gold covered

    def test_order_matters_for_correct_path(self):
        """Same endpoints but wrong order is NOT a valid subsequence -> CP False."""
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/me"},
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/search"},
        ]
        oracle = ["GET /search", "GET /me"]
        result = compare_plan_with_oracle(tasks, oracle)
        self.assertFalse(result["is_correct_path"])          # order violated
        # but step-level coverage is still perfect (order-independent):
        self.assertEqual(result["step_recall"], 1.0)

    def test_partial_step_recall(self):
        """Two of three gold endpoints selected -> recall 2/3, CP False."""
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/TMDB/1.0/search/movie"},
            {"operation": "GET", "url": "http://localhost/rest/TMDB/1.0/movie/{{m.id}}/credits"},
        ]
        oracle = [
            "GET /search/movie",
            "GET /movie/{movie_id}",
            "GET /movie/{movie_id}/credits",
        ]
        result = compare_plan_with_oracle(tasks, oracle)
        self.assertFalse(result["is_correct_path"])
        self.assertEqual(result["step_hits"], 2)
        self.assertAlmostEqual(result["step_recall"], round(2 / 3, 4))

    def test_step_missed_lists_only_truly_absent_endpoints(self):
        """oracle_missed (ordered) over-reports; step_missed lists the real gap."""
        tasks = [
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/search"},
            {"operation": "GET", "url": "http://localhost/rest/Spotify/1.0/artists/{{x.id}}/top-tracks"},
            {"operation": "POST", "url": "http://localhost/rest/Spotify/1.0/users/me/playlists"},
            {"operation": "POST", "url": "http://localhost/rest/Spotify/1.0/playlists/{{p.id}}/tracks"},
        ]
        oracle = [
            "GET /search",
            "GET /me",
            "POST /users/{user_id}/playlists",
            "POST /playlists/{playlist_id}/tracks",
        ]
        result = compare_plan_with_oracle(tasks, oracle)
        self.assertFalse(result["is_correct_path"])
        # ordered view breaks at /me and reports 3 missed:
        self.assertEqual(len(result["oracle_missed"]), 3)
        # order-independent view: only /me is truly absent:
        self.assertEqual(len(result["step_missed"]), 1)
        self.assertEqual(result["step_missed"][0]["oracle"], ("GET", "/me"))


if __name__ == "__main__":
    unittest.main()