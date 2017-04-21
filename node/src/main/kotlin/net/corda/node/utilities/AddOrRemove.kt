package net.corda.node.utilities

import net.corda.core.ErrorOr
import net.corda.core.serialization.CordaSerializable

/**
 * Enum for when adding/removing something, for example adding or removing an entry in a directory.
 */
@CordaSerializable
enum class AddOrRemove {
    ADD,
    REMOVE
}

/**
 * Class for when adding/removing something with a reason for removal (normal or error). Used for state machines.
 */
@CordaSerializable
sealed class AddOrRemoveError {
    object Add: AddOrRemoveError()
    class Remove(val reason: ErrorOr<String>): AddOrRemoveError()
}
