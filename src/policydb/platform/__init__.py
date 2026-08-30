"""CRPD unified platform layer (additive).

Wraps existing policydb modules behind the 12 core seams. Existing modules are
NOT modified; this layer only adapts and orchestrates them, and records which
seam maps to which existing implementation.
"""
