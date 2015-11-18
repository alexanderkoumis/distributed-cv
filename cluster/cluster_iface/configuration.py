"""A configuration module.
"""
import os
import yaml

from security_context import AwsSecurityContext

class Configuration(object):
    """Abstract Configuration class.
    """
    def __init__(self):
        pass

class AwsConfiguration(Configuration):
    """An AwsConfiguration for environment configuration control.
    """
    AWS_SEC = AwsSecurityContext()
    CONFIG_FILE = '.mrjob.conf'
    PEM_KEYS = '.ssh/key_pair2.pem'
    CONFIGURATIONS = {
        'cpu': { 
            'runners': {
                'emr': {
                    'bootstrap': [
                        'wget http://facedata.s3.amazonaws.com/OpenCV-unknown-x86_64.tar.gz && sudo tar -xzvf ./OpenCV-unknown-x86_64.tar.gz -C /usr/local',
                        'export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig && wget http://facedata.s3.amazonaws.com/opencv-gpu-py.tar.gz && sudo -E pip install ./opencv-gpu-py.tar.gz'
                    ],
                    'setup': [
                        'export PYTHONPATH=$PYTHONPATH:/usr/local/lib/python2.6/dist-packages/:/usr/local/lib64/python2.6/site-packages',
                        'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib'
                    ],
                    'cleanup': [
                        'NONE'
                    ],
    
                    ##### Cost Factors #####
                    'num_ec2_instances': 2,
                    'ec2_master_instance_type': 'm1.medium',
                    'ec2_slave_instance_type': 'm1.medium',
                    'max_hours_idle': 1,
                    'mins_to_end_of_hour': 10,
                    'pool_emr_job_flows': True,
    
                    ##### Security Factors #####
                    'ec2_key_pair': 'key_pair2',
                    'ec2_key_pair_file': os.path.join(AWS_SEC.HOME, PEM_KEYS),
    
                    ##### Other #####
                    'label': 'mcmc_konix',
                    'ssh_tunnel': True,
                    'ssh_tunnel_is_open': True,
                    'ssh_tunnel_to_job_tracker': True,
                    'visible_to_all_users': True,
                    'ami_version': '3.10.0',
                    'check_emr_status_every': 5,
                    'enable_emr_debugging': True,
                    'emr_action_on_failure': 'CONTINUE'
                    # 'python_archives': None
                },
                'inline': {
                    'base_tmp_dir': os.path.join(AWS_SEC.HOME, '.tmp')
                }
            }
        },
        'gpu': { 
            'runners': {
                'emr': {
                    'bootstrap': [
                        'BOOTSTRAP_DIR=/mnt/bootstrap',
                        'OPENCV=OpenCV-3.0.0-AmazonLinux2015.03-GPU-x86_64.tar.gz',
                        'OPENCV_GPU=opencv-gpu-py.tar.gz',
                        'CUDA_PKG=cuda_7.5.18_linux.run',
                        'CUDA_BIN=cuda-linux64-rel-7.5.18-19867135.run',
                        'NVIDIA_BIN=NVIDIA-Linux-x86_64-352.55.run',
                        'BUCKET_URL=http://facedata.s3.amazonaws.com',
                        'sudo yum -y update',
                        'sudo pip -v install --upgrade numpy',
                        'mkdir -p $BOOTSTRAP_DIR/nvidia',
                        'wget http://us.download.nvidia.com/XFree86/Linux-x86_64/352.55/$NVIDIA_BIN -P $BOOTSTRAP_DIR/nvidia && chmod +x $BOOTSTRAP_DIR/nvidia/$NVIDIA_BIN',
                        'sudo $BOOTSTRAP_DIR/nvidia/$NVIDIA_BIN -s -N --no-kernel-module && rm $BOOTSTRAP_DIR/nvidia/$NVIDIA_BIN',
                        'wget http://developer.download.nvidia.com/compute/cuda/7.5/Prod/local_installers/$CUDA_PKG -P $BOOTSTRAP_DIR/nvidia && chmod +x $BOOTSTRAP_DIR/nvidia/$CUDA_PKG',
                        '$BOOTSTRAP_DIR/nvidia/$CUDA_PKG -extract=$BOOTSTRAP_DIR/nvidia && rm $BOOTSTRAP_DIR/nvidia/$CUDA_PKG',
                        'sudo $BOOTSTRAP_DIR/nvidia/$CUDA_BIN -noprompt && rm $BOOTSTRAP_DIR/nvidia/$CUDA_BIN',
                        'wget $BUCKET_URL/$OPENCV && sudo tar -xzvf ./$OPENCV -C /usr/local',
                        'export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig && wget $BUCKET_URL/$OPENCV_GPU && sudo -E pip install ./$OPENCV_GPU'
                    ],
                    'setup': [
                        'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/cuda/lib',
                        'export PATH=$PATH:/usr/local/cuda/bin',
                        'export PYTHONPATH=$PYTHONPATH:/usr/local/lib/python2.6/dist-packages/:/usr/local/lib64/python2.6/site-packages:/usr/local/lib/python2.7/dist-packages/:/usr/local/lib64/python2.7/site-packages',
                    ],
                    'cleanup': [
                        'NONE'
                    ],
    
                    ##### Cost Factors #####
                    'num_ec2_instances': 2,
                    'ec2_master_instance_type': 'g2.2xlarge',
                    'ec2_slave_instance_type': 'g2.2xlarge',
                    'ec2_core_instance_type': 'g2.2xlarge',
                    'ec2_master_instance_bid_price': '0.08',
                    'ec2_slave_instance_type': '0.08',
                    'ec2_core_instance_bid_price': '0.08',
                    'max_hours_idle': 1,
                    'mins_to_end_of_hour': 10,
                    'pool_emr_job_flows': True,
    
                    ##### Security Factors #####
                    'ec2_key_pair': 'key_pair2',
                    'ec2_key_pair_file': os.path.join(AWS_SEC.HOME, PEM_KEYS),
    
                    ##### Other #####
                    'label': 'mcmc_konix',
                    'ssh_tunnel': True,
                    'ssh_tunnel_is_open': True,
                    'ssh_tunnel_to_job_tracker': True,
                    'visible_to_all_users': True,
                    'ami_version': '3.10.0',
                    'check_emr_status_every': 5,
                    'enable_emr_debugging': True,
                    'emr_action_on_failure': 'CONTINUE'
                    # 'python_archives': None
                },
                'inline': {
                    'base_tmp_dir': os.path.join(AWS_SEC.HOME, '.tmp')
                }
            }
        }
    }
    if 'matt' not in os.path.basename(AWS_SEC.HOME):
        # If it's not matt it's alex. Hopefully.
        CONFIGURATIONS['cpu']['runners']['emr']['owner'] = 'konixmusic'
        CONFIGURATIONS['gpu']['runners']['emr']['owner'] = 'konixmusic'

    # The configured/formatted string resrouce.
    mrjob_conf = ''

    def __init__(self, cpu_or_gpu):
        self._cpu_or_gpu = 'gpu' if cpu_or_gpu == 'gpu' else 'cpu'
        self.conf_file = os.path.join(self.AWS_SEC.HOME, self.CONFIG_FILE)
        if not self.mrjob_conf:
            # We haven't config'd env b/c don't have the config'd format string.
            config = self.CONFIGURATIONS[self._cpu_or_gpu]
            self.mrjob_conf = self._install_config(config)
        # Control the environment.
        self._master_config_ctrlr()

    def _install_config(self, config):
        """Dump the config file.
        """
        return yaml.dump(config, default_flow_style=False)

    def _master_config_ctrlr(self):
        """Master environment config controller.
        """
        self._setenv_mrjob_config()
        self._setenv_aws_config()

    def _setenv_aws_config(self):
        """AWS uses a hidden directory with config files.
        """
        aws_hdir = '.aws'
        conf_file = 'config'
        cred_file = 'credentials'
        acc_kid = 'aws_access_key_id'
        sec_kid = 'aws_secret_access_key'

        aws_path = os.path.join(self.AWS_SEC.HOME, aws_hdir)
        if not os.path.isdir(aws_path):
            # We cant modify files in an dir that doesn't exist.
            os.mkdir(aws_path)

        aws_conf = os.path.join(aws_path, conf_file)
        if not os.path.isfile(aws_conf):
            # If the conf file doesn't exist write to it.
            conf = '[default]\nregion = us-east-1\n'
            with open(aws_conf, 'w+') as file_d:
                file_d.write(conf)

        aws_cred = os.path.join(aws_path, cred_file)
        if not os.path.isfile(aws_cred):
            # If the cred file doesn't exist write to it.
            cred = '[default]\n{} = {}\n{} = {}\n'
            data = cred.format(acc_kid,
                               self.AWS_SEC.access_key_id,
                               sec_kid,
                               self.AWS_SEC.secret_access_key)
            with open(aws_cred, 'w+') as file_d:
                file_d.write(data)

    def _setenv_mrjob_config(self):
        """Write the mrjob configuration file.
        """
        if not os.path.isfile(self.conf_file):
            # If it doesn't exist write it.
            with open(self.conf_file, 'w') as file_d:
                file_d.write(self.mrjob_conf)
        else:
            with open(self.conf_file, 'w+') as file_d:
                config = file_d.read()
                if config != self.conf_file:
                    # If it exists, but outdated, write the new config.
                    file_d.write(self.mrjob_conf)


if __name__ == '__main__':
    AWS_CONF = AwsConfiguration()


