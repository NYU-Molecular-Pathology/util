I recently needed a way to run unit tests on Travis for a project that uses Sun Grid Engine, [Grid Map](https://github.com/EducationalTestingService/gridmap).  Unfortunately, it seemed like no one had figured out how to set that up on Travis before (or simply create a single-machine installation without any user interaction). After hours of trial-and-error, I now know the secrets to making a single-machine installation of SGE that runs on Travis, and I'm sharing my script to prevent other people from going through the same frustrating experience.

To use the `install_sge.sh` script below, you just need to copy all of the files in this gist to a `travis` sub-directory directly under the root of your GitHub project, and add the following lines to your `.travis.yml`
```yaml
before_install:
  - travis/install_sge.sh
  - export SGE_ROOT=/var/lib/gridengine
  - export SGE_CELL=default
  - export DRMAA_LIBRARY_PATH=/usr/lib/libdrmaa.so.1.0
```

Once you've done that, you should be able to use qsub, or any libraries that use DRMAA to talk to the grid engine.

How it works
------------
If you care about what the script actually does, we first modify the `/etc/hosts` file to make sure that the VM's hostname maps back to 127.0.0.1 to prevent SGE from complaining that localhost doesn't point to the same IP as the hostname. Then, we install SGE by pre-specifying some options using `debconf-set-selections`. After that, we determine who the current user is and how many cores the current machine has, and modify the setting template files to reflect that. Finally, we apply the settings in the template files and print out some debugging info to make sure the grid started properly.

The weirdest issue I had when creating this was that sometimes when I tried to add localhost as an execution host, I would get the error that it already was one, so that's why there's that rather crazy-looking check to see if that's the case.

Additional files (only shows up on [roughdraft.io](http://dan-blanchard.roughdraft.io/6586533))
-----------------------------------------------------------------------------------------------

**install_sge.sh**

<gist>install_sge.sh</gist>

**host_template**

<gist>host_template</gist>

**queue_template**

<gist>queue_template</gist>

**smp_template**

<gist>smp_template</gist>
