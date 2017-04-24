package net.corda.demobench.model

import net.corda.core.crypto.X509Utilities
import org.bouncycastle.asn1.x500.X500Name
import org.junit.Test
import kotlin.test.assertEquals

class NetworkMapConfigTest {

    @Test
    fun keyValue() {
        val config = NetworkMapConfig(X500Name("CN=My\tNasty Little\rLabel,O=R3,OU=corda,L=London,C=UK\n"), 10000)
        assertEquals("mynastylittlelabel", config.key)
    }

    @Test
    fun removeWhitespace() {
        assertEquals("OneTwoThreeFour!", "One\tTwo   \rThree\r\nFour!".stripWhitespace())
    }

}
