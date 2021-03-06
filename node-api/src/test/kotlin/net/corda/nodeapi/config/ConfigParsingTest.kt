package net.corda.nodeapi.config

import com.google.common.net.HostAndPort
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory.empty
import com.typesafe.config.ConfigRenderOptions.defaults
import com.typesafe.config.ConfigValueFactory
import net.corda.core.div
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.util.*
import kotlin.reflect.full.primaryConstructor

class ConfigParsingTest {
    @Test
    fun `String`() {
        testPropertyType<StringData, StringListData, String>("hello world!", "bye")
    }

    @Test
    fun `Int`() {
        testPropertyType<IntData, IntListData, Int>(1, 2)
    }

    @Test
    fun `Long`() {
        testPropertyType<LongData, LongListData, Long>(Long.MAX_VALUE, Long.MIN_VALUE)
    }

    @Test
    fun `Double`() {
        testPropertyType<DoubleData, DoubleListData, Double>(1.2, 3.4)
    }

    @Test
    fun `Boolean`() {
        testPropertyType<BooleanData, BooleanListData, Boolean>(true, false)
    }

    @Test
    fun `Enum`() {
        testPropertyType<EnumData, EnumListData, TestEnum>(TestEnum.Value2, TestEnum.Value1, valuesToString = true)
    }

    @Test
    fun `LocalDate`() {
        testPropertyType<LocalDateData, LocalDateListData, LocalDate>(LocalDate.now(), LocalDate.now().plusDays(1), valuesToString = true)
    }

    @Test
    fun `Instant`() {
        testPropertyType<InstantData, InstantListData, Instant>(Instant.now(), Instant.now().plusMillis(100), valuesToString = true)
    }

    @Test
    fun `HostAndPort`() {
        testPropertyType<HostAndPortData, HostAndPortListData, HostAndPort>(
                HostAndPort.fromParts("localhost", 2223),
                HostAndPort.fromParts("localhost", 2225),
                valuesToString = true)
    }

    @Test
    fun `Path`() {
        val path = Paths.get("tmp") / "test"
        testPropertyType<PathData, PathListData, Path>(path, path / "file", valuesToString = true)
    }

    @Test
    fun `URL`() {
        testPropertyType<URLData, URLListData, URL>(URL("http://localhost:1234"), URL("http://localhost:1235"), valuesToString = true)
    }

    @Test
    fun `flat Properties`() {
        val config = config("value" to mapOf("key" to "prop"))
        assertThat(config.parseAs<PropertiesData>().value).isEqualTo(Properties().apply { this["key"] = "prop" })
    }

    @Test
    fun `Properties key with dot`() {
        val config = config("value" to mapOf("key.key2" to "prop"))
        assertThat(config.parseAs<PropertiesData>().value).isEqualTo(Properties().apply { this["key.key2"] = "prop" })
    }

    @Test
    fun `nested Properties`() {
        val config = config("value" to mapOf("first" to mapOf("second" to "prop")))
        assertThat(config.parseAs<PropertiesData>().value).isEqualTo(Properties().apply { this["first.second"] = "prop" })
    }

    @Test
    fun `List of Properties`() {
        val config = config("values" to listOf(emptyMap(), mapOf("key" to "prop")))
        assertThat(config.parseAs<PropertiesListData>().values).containsExactly(
                Properties(),
                Properties().apply { this["key"] = "prop" })
    }

    @Test
    fun `Set`() {
        val config = config("values" to listOf("a", "a", "b"))
        assertThat(config.parseAs<StringSetData>().values).containsOnly("a", "b")
        assertThat(empty().parseAs<StringSetData>().values).isEmpty()
    }

    @Test
    fun `multi property data class`() {
        val data = config(
                "b" to true,
                "i" to 123,
                "l" to listOf("a", "b"))
                .parseAs<MultiPropertyData>()
        assertThat(data.i).isEqualTo(123)
        assertThat(data.b).isTrue()
        assertThat(data.l).containsExactly("a", "b")
    }

    @Test
    fun `nested data classes`() {
        val config = config(
                "first" to mapOf(
                        "value" to "nested"))
        val data = NestedData(StringData("nested"))
        assertThat(config.parseAs<NestedData>()).isEqualTo(data)
    }

    @Test
    fun `List of data classes`() {
        val config = config(
                "list" to listOf(
                        mapOf("value" to "1"),
                        mapOf("value" to "2")))
        val data = DataListData(listOf(StringData("1"), StringData("2")))
        assertThat(config.parseAs<DataListData>()).isEqualTo(data)
    }

    @Test
    fun `default value property`() {
        assertThat(config("a" to 3).parseAs<DefaultData>()).isEqualTo(DefaultData(3, 2))
        assertThat(config("a" to 3, "defaultOfTwo" to 3).parseAs<DefaultData>()).isEqualTo(DefaultData(3, 3))
    }

    @Test
    fun `nullable property`() {
        assertThat(empty().parseAs<NullableData>().nullable).isNull()
        assertThat(config("nullable" to null).parseAs<NullableData>().nullable).isNull()
        assertThat(config("nullable" to "not null").parseAs<NullableData>().nullable).isEqualTo("not null")
    }

    @Test
    fun `old config property`() {
        assertThat(config("oldValue" to "old").parseAs<OldData>().newValue).isEqualTo("old")
        assertThat(config("newValue" to "new").parseAs<OldData>().newValue).isEqualTo("new")
    }

    private inline fun <reified S : SingleData<V>, reified L : ListData<V>, V : Any> testPropertyType(
            value1: V,
            value2: V,
            valuesToString: Boolean = false) {
        testSingleProperty<S, V>(value1, valuesToString)
        testListProperty<L, V>(value1, value2, valuesToString)
    }

    private inline fun <reified T : SingleData<V>, V : Any> testSingleProperty(value: V, valueToString: Boolean) {
        val constructor = T::class.primaryConstructor!!
        val config = config("value" to if (valueToString) value.toString() else value)
        val data = constructor.call(value)
        assertThat(config.parseAs<T>().value).isEqualTo(data.value)
    }

    private inline fun <reified T : ListData<V>, V : Any> testListProperty(value1: V, value2: V, valuesToString: Boolean) {
        val rawValues = listOf(value1, value2)
        val configValues = if (valuesToString) rawValues.map(Any::toString) else rawValues
        val constructor = T::class.primaryConstructor!!
        for (n in 0..2) {
            val config = config("values" to configValues.take(n))
            val data = constructor.call(rawValues.take(n))
            assertThat(config.parseAs<T>().values).isEqualTo(data.values)
        }
        assertThat(empty().parseAs<T>().values).isEmpty()
    }

    private fun config(vararg values: Pair<String, *>): Config {
        val config = ConfigValueFactory.fromMap(mapOf(*values))
        println(config.render(defaults().setOriginComments(false)))
        return config.toConfig()
    }

    private interface SingleData<out T> {
        val value: T
    }

    private interface ListData<out T> {
        val values: List<T>
    }

    data class StringData(override val value: String) : SingleData<String>
    data class StringListData(override val values: List<String>) : ListData<String>
    data class StringSetData(val values: Set<String>)
    data class IntData(override val value: Int) : SingleData<Int>
    data class IntListData(override val values: List<Int>) : ListData<Int>
    data class LongData(override val value: Long) : SingleData<Long>
    data class LongListData(override val values: List<Long>) : ListData<Long>
    data class DoubleData(override val value: Double) : SingleData<Double>
    data class DoubleListData(override val values: List<Double>) : ListData<Double>
    data class BooleanData(override val value: Boolean) : SingleData<Boolean>
    data class BooleanListData(override val values: List<Boolean>) : ListData<Boolean>
    data class EnumData(override val value: TestEnum) : SingleData<TestEnum>
    data class EnumListData(override val values: List<TestEnum>) : ListData<TestEnum>
    data class LocalDateData(override val value: LocalDate) : SingleData<LocalDate>
    data class LocalDateListData(override val values: List<LocalDate>) : ListData<LocalDate>
    data class InstantData(override val value: Instant) : SingleData<Instant>
    data class InstantListData(override val values: List<Instant>) : ListData<Instant>
    data class HostAndPortData(override val value: HostAndPort) : SingleData<HostAndPort>
    data class HostAndPortListData(override val values: List<HostAndPort>) : ListData<HostAndPort>
    data class PathData(override val value: Path) : SingleData<Path>
    data class PathListData(override val values: List<Path>) : ListData<Path>
    data class URLData(override val value: URL) : SingleData<URL>
    data class URLListData(override val values: List<URL>) : ListData<URL>
    data class PropertiesData(override val value: Properties) : SingleData<Properties>
    data class PropertiesListData(override val values: List<Properties>) : ListData<Properties>
    data class MultiPropertyData(val i: Int, val b: Boolean, val l: List<String>)
    data class NestedData(val first: StringData)
    data class DataListData(val list: List<StringData>)
    data class DefaultData(val a: Int, val defaultOfTwo: Int = 2)
    data class NullableData(val nullable: String?)
    data class OldData(
            @OldConfig("oldValue")
            val newValue: String)

    enum class TestEnum { Value1, Value2 }

}