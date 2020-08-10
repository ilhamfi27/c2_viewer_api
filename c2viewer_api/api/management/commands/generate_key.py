from django.core.management.base import BaseCommand, CommandError
import os
import random

class Command(BaseCommand):
    help = 'Command for generating JWT user key and JWT token key'

    def handle(self, *args, **options):
        BASE_DIR = os.path.abspath(os.path.dirname(__name__))
        path = os.path.join(BASE_DIR, '.env')
        self.stdout.write(path, ending='')
        generated = self.write_generated_token(path)
        if generated:
            self.stdout.write("\nJWT Token Generated", ending='')
        else:
            self.stdout.write("\nJWT Token Generate Failed", ending='')

    def jwt_random_string_generator(self):
        chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ#$%&*()+-=@^_~"
        return ''.join(random.choice(chars) for x in range(64))

    def write_generated_token(self, path):
        try:
            reader = open(path, 'r')
            env_vars = reader.readlines()
            new_vars = []
            # print(env_vars)
            for c in env_vars:
                if "JWT_TOKEN_KEY" in c or "JWT_USER_KEY" in c:
                    x = c.split("=")
                    x[1] = self.jwt_random_string_generator()
                    c = "=".join([x[0], '"'+x[1] + '"\n'])
                new_vars.append(c)

            with open(path, 'w') as writer:
                for vars in new_vars:
                    writer.write(vars)
            return True
        except FileNotFoundError as e:
            print("No Such File", e)
        except Exception as e:
            print(e)
        return False