# user account to run SGE under
sge_user="$1"

/etc/init.d/gridengine-master start
/etc/init.d/gridengine-exec start
sudo -u sgeadmin qconf -am "$sge_user"
sudo -u "$sge_user" qconf -au "$sge_user" users
sudo -u "$sge_user" qconf -Ae /exec_host
sudo -u "$sge_user" qconf -Ahgrp /host_group_entry
sudo -u "$sge_user" qconf -aattr hostgroup hostlist "$HOSTNAME" @allhosts
sudo -u "$sge_user" qconf -Aq /queue
sudo -u "$sge_user" qconf -aattr queue hostlist @allhosts main.q
sudo -u "$sge_user" qconf -as "$HOSTNAME"

# set maximum of avaiable CPU's
sed -i "s|processors            4|processors            `num=$(grep ^processor /proc/cpuinfo | wc -l) && echo $((num-1))`|g" /queue
sed -i "s|slots                 4|slots                 `num=$(grep ^processor /proc/cpuinfo | wc -l) && echo $((num-1))`|g" /queue


# printf "\nTest the container by logging into the SGE user account and submitting a test job:\n\n%s %s\n\n" "sudo -u $sge_user" '/test.sh'
