Dockerfile to build a container with SGE and Python installed.

# Build

To build type:

```
git clone git@github.com:NYU-Molecular-Pathology/util.git
cd util/Docker
docker build -t stevekm/python-util .
```

# Run

```
docker run --rm -t -i stevekm/python-util
```

# Test `util`

From inside the container:

```
cd util/
python test.py
```

# Test SGE

From inside the container, run `/test.sh`:

```
root@7022b3a28667:/# /test.sh
Your job 1 ("STDIN") has been submitted
job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
-----------------------------------------------------------------------------------------------------------------
      1 0.00000 STDIN      root         qw    12/07/2017 23:58:38                                    1
```

# References

Based on Dockerfile hosted [here](https://github.com/bgruening/docker-recipes/blob/621e80e37d1829494bc193ce3f20fe7f4833ec2a/freiburger-rna-tools/Dockerfile#L14
)
