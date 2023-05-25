import os
import sys

to_insert = os.path.abspath(os.path.join(
                os.path.dirname(__file__), '../../'))

print(to_insert)

sys.path.insert(0, to_insert)
