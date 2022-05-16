from functools import lru_cache

def levenshtein_distance(a, b):

    len_a = len(a)
    len_b = len(b)

    @lru_cache(None)
    def min_dist(s1, s2):

        if s1 == len_a or s2 == len_b:
            return len_a - s1 + len_b - s2

        if a[s1] == b[s2]:
            return min_dist(s1 + 1, s2 + 1)

        return 1 + min(
            min_dist(s1, s2 + 1),      # cost of insert
            min_dist(s1 + 1, s2),      # cost of delete
            min_dist(s1 + 1, s2 + 1),  # cost of replace
        )

    return min_dist(0, 0)

def distance_percent(a, b):
    max_len = max(1, len(a), len(b))
    dist = levenshtein_distance(a, b)
    return 100.0 * ((max_len - dist) / max_len)

#print(levenshtein_word_distance(["cat", "fish", "in", "water"], ["tuna", "fish", "in", "oil"])) 
#print(levenshtein_word_distance("tuna fish in water", "tuna fish in oil")) 
print(distance_percent("tuna fish in water", "tuna fish in oil")) 
