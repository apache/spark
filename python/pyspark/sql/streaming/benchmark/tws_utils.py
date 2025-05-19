#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql.streaming.list_state_client import ListStateClient
from pyspark.sql.streaming.map_state_client import MapStateClient
from pyspark.sql.streaming.value_state_client import ValueStateClient
from pyspark.sql.streaming.stateful_processor import ListState, MapState, ValueState


def get_value_state(api_client, state_name, value_schema):
    api_client.get_value_state(
        state_name,
        schema=value_schema,
        ttl_duration_ms=None,
    )

    value_state_client = ValueStateClient(api_client, schema=value_schema)

    return ValueState(value_state_client, state_name)


def get_list_state(api_client, state_name, list_element_schema):
    api_client.get_list_state(
        state_name,
        schema=list_element_schema,
        ttl_duration_ms=None,
    )

    list_state_client = ListStateClient(api_client, schema=list_element_schema)

    return ListState(list_state_client, state_name)


def get_map_state(api_client, state_name, map_key_schema, map_value_schema):
    api_client.get_map_state(
        state_name,
        user_key_schema=map_key_schema,
        value_schema=map_value_schema,
        ttl_duration_ms=None,
    )

    map_state_client = MapStateClient(
        api_client,
        user_key_schema=map_key_schema,
        value_schema=map_value_schema,
    )

    return MapState(map_state_client, state_name)
