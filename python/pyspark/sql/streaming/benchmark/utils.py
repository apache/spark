#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import numpy as np


def print_percentiles(values, percentiles):
    percentile_values = np.percentile(values, q=percentiles)
    print("\t\t".join([f"perc:{x}" for x in percentiles]))
    print("\t\t".join(["{:.3f}".format(x) for x in percentile_values]))
