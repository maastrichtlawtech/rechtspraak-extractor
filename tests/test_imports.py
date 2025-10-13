"""
Test module to verify import behavior matches README documentation.
"""
import pytest


def test_package_level_import():
    """Test that functions are accessible at package level as shown in README."""
    import rechtspraak_extractor as rex
    
    # Verify functions are accessible
    assert hasattr(rex, 'get_rechtspraak'), "get_rechtspraak not found at package level"
    assert hasattr(rex, 'get_rechtspraak_metadata'), "get_rechtspraak_metadata not found at package level"
    
    # Verify they are callable
    assert callable(rex.get_rechtspraak), "get_rechtspraak is not callable"
    assert callable(rex.get_rechtspraak_metadata), "get_rechtspraak_metadata is not callable"


def test_direct_module_import():
    """Test that direct imports still work (backwards compatibility)."""
    from rechtspraak_extractor.rechtspraak import get_rechtspraak
    from rechtspraak_extractor.rechtspraak_metadata import get_rechtspraak_metadata
    
    assert callable(get_rechtspraak), "get_rechtspraak not callable from direct import"
    assert callable(get_rechtspraak_metadata), "get_rechtspraak_metadata not callable from direct import"


def test_module_access():
    """Test that submodules are still accessible."""
    import rechtspraak_extractor as rex
    
    # Verify submodules are accessible
    assert hasattr(rex, 'rechtspraak'), "rechtspraak module not accessible"
    assert hasattr(rex, 'rechtspraak_metadata'), "rechtspraak_metadata module not accessible"
    assert hasattr(rex, 'rechtspraak_functions'), "rechtspraak_functions module not accessible"
    
    # Verify functions in submodules
    assert hasattr(rex.rechtspraak, 'get_rechtspraak'), "get_rechtspraak not in rechtspraak module"
    assert hasattr(rex.rechtspraak_metadata, 'get_rechtspraak_metadata'), "get_rechtspraak_metadata not in rechtspraak_metadata module"


def test_all_exports():
    """Test that __all__ is properly defined."""
    import rechtspraak_extractor as rex
    
    assert hasattr(rex, '__all__'), "__all__ not defined"
    assert 'get_rechtspraak' in rex.__all__, "get_rechtspraak not in __all__"
    assert 'get_rechtspraak_metadata' in rex.__all__, "get_rechtspraak_metadata not in __all__"


def test_function_signatures():
    """Test that functions have the expected signatures."""
    import inspect
    import rechtspraak_extractor as rex
    
    # Test get_rechtspraak signature
    sig = inspect.signature(rex.get_rechtspraak)
    params = list(sig.parameters.keys())
    assert 'max_ecli' in params, "max_ecli parameter missing from get_rechtspraak"
    assert 'sd' in params, "sd parameter missing from get_rechtspraak"
    assert 'ed' in params, "ed parameter missing from get_rechtspraak"
    assert 'save_file' in params, "save_file parameter missing from get_rechtspraak"
    
    # Test get_rechtspraak_metadata signature
    sig = inspect.signature(rex.get_rechtspraak_metadata)
    params = list(sig.parameters.keys())
    assert 'save_file' in params, "save_file parameter missing from get_rechtspraak_metadata"
    assert 'dataframe' in params, "dataframe parameter missing from get_rechtspraak_metadata"
    assert 'filename' in params, "filename parameter missing from get_rechtspraak_metadata"


if __name__ == "__main__":
    # Run tests manually
    test_package_level_import()
    print("✓ Package level import test passed")
    
    test_direct_module_import()
    print("✓ Direct module import test passed")
    
    test_module_access()
    print("✓ Module access test passed")
    
    test_all_exports()
    print("✓ __all__ exports test passed")
    
    test_function_signatures()
    print("✓ Function signatures test passed")
    
    print("\nAll import tests passed!")
