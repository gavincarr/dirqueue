
Overview
--------

This is a stripped-down partial port of Justin Mason's
[IPC::DirQueue perl module](https://github.com/jmason/IPC-DirQueue)
to Go.

The initial goal is just to implement Go equivalents to IPC::DirQueue's
`enqueue_{file,fh,string}` methods, allowing Go utilities and daemons
to submit data files to existing IPC::DirQueue queues without having
to call out externally to perl.


Installation
------------

    go get github.com/gavincarr/dirqueue


API
---

    # Constructor - requires path to queue
    dq, err := dirqueue.New("/path/to/queue")
    if err != nil { ... }

    # Add options (metadata and priorities only, for now), if required
    dqopt := dirqueue.DefaultOptions()
    dqopt.Metadata["uuid"] = "84b83cbe-4d7c-4338-b3b5-a99eb5ea671d"
    dqopt.Metadata["foo"] = "12345"
    dqopt.Priority = 30

    # Enqueue from file, without options
    err = dq.EnqueueFile("/path/to/file", nil)

    # Enqueue from reader or filehandle, with explicit options
    err = dq.EnqueueReader(filehandle, dqopt)

    # Enqueue from string data, with explicit options
    err = dq.EnqueueString("Here lies the data.\n", dqopt)


Copyright and Licence
---------------------

Copyright 2021 Gavin Carr <gavin@openfusion.com.au>.

This project is licensed under the terms of the MIT licence.

