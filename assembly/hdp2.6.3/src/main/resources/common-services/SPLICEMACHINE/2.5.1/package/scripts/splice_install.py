import sys
import sys
from resource_management import *

reload(sys)
sys.setdefaultencoding('utf8')

class SpliceInstall(Script):
  def install(self, env):
    import params
    self.install_packages(env)
    env.set_params(params)
    print(params)
    print 'Install the client';

#    dir = '/var/lib/splicemachine'
#    if os.path.exists(dir):
#      shutil.rmtree(dir)
#    os.makedirs(dir)

  def configure(self, env):
    print 'Configure the client';
  def somethingcustom(self, env):
    print 'Something custom';

if __name__ == "__main__":
  SpliceInstall().execute()