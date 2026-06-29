// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Internal;

/// <summary>Argument-validation helpers that work across all target frameworks (including netstandard2.0).</summary>
internal static class Check
{
    /// <summary>Throws <see cref="ArgumentNullException"/> when <paramref name="argument"/> is null.</summary>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="paramName">The argument name, captured automatically.</param>
    public static void NotNull(
        object? argument,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(argument))] string? paramName = null)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(argument, paramName);
#else
        if (argument is null)
        {
            throw new ArgumentNullException(paramName);
        }
#endif
    }
}
