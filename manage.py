debug = False

if debug:
    from gsp import main
    main.main()

else:
    import sys
    import importlib

    print(sys.argv)
    print(sys.argv[0])
    print(sys.argv[1])
    print(sys.argv[2])
    print(sys.argv[3])

    sys.path.insert(0, './' + sys.argv[1])

    try:
        importModule = importlib.import_module(sys.argv[2])
        func = getattr(importModule, sys.argv[3])
        func()
    except Exception as e:
        print(e)
