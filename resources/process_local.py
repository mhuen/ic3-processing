import os
import subprocess
import glob
import signal
import sys
import copy

import click


class JobLogBook(object):
    def __init__(self, n_jobs=1, log_dir=None):
        self.log_dir = log_dir
        self.logbook = {}
        self.n_jobs = n_jobs
        self.running_pid = []
        self.n_finished = 0
        self.log = []
        self._original_sigint = None

    def process(self, binaries):
        self.__binaries = copy.copy(binaries)
        click.echo(
            "Processing {} with max. {} parallel jobs!".format(
                len(binaries), self.n_jobs
            )
        )

        with click.progressbar(length=len(binaries)) as bar:
            bar.update(self.n_finished)
            for i, job in enumerate(binaries):
                if os.path.isfile(job) and os.access(job, os.X_OK):
                    self.__start_subprocess__(job)
                else:
                    click.echo("{} is not executable! (Skipped)".format(job))
                    self.n_finished += 1
                    self.log.append([job, "not_executable"])
                    del self.__binaries[i]
                if len(self.running_pid) >= self.n_jobs:
                    self.__wait__(bar)
            click.echo("\nAll Jobs started. Wait for last jobs to finish!")
            while len(self.running_pid) > 0:
                self.__wait__(bar)
            bar.update(self.n_finished)
        click.echo("Finished!")
        if self.log_dir is not None:
            self.__store__()

    def __wait__(self, progressbar=None):
        try:
            pid, exit_code = os.wait()
        except OSError:
            pid, exit_code = os.wait()
        job_file = self.logbook[pid][1]
        click.echo(
            "\n{} finished with exit code {}".format(job_file, exit_code)
        )
        self.log.append([job_file, exit_code])
        self.n_finished += 1
        self.__clear_job__(pid)
        if progressbar is not None:
            progressbar.update(1)

    def __wait_rest__(self, save=False):
        click.echo("waiting for all subprocesses to finish")

        def exit_with_pid_term(signum, frame):
            for pid in self.running_pid:
                os.kill(pid, signal.SIGINT)
            sys.exit(1)

        signal.signal(signal.SIGINT, exit_with_pid_term)

        for pid in self.running_pid:
            os.kill(pid, signal.SIGCONT)
        while True:
            try:
                self.__wait__()
            except OSError:
                break
        if save:
            self.__store__()

    def __store__(self):
        path = os.path.join(self.log_dir, "resume.txt")
        binary_set = set(self.__binaries)
        finished_jobs = set([log_i[0] for log_i in self.log])
        unfinished_jobs = binary_set.difference(finished_jobs)
        with open(path, "w") as f:
            for job, exit_code in self.log:
                f.write("{};{}\n".format(job, exit_code))
            for job in unfinished_jobs:
                f.write("{};\n".format(job))

    def resume(self, resume_file):
        with open(resume_file) as f:
            content = f.readlines()
        content = [c.strip() for c in content]
        retry = click.confirm("Retry failed jobs?")
        binaries = []
        for c in content:
            try:
                job, exit_code = c.split(";")
            except ValueError:
                raise ValueError("{} can be resumed!".format(resume_file))
            if retry:
                if exit_code != "0":
                    binaries.append(job)
            else:
                if exit_code == "":
                    binaries.append(job)
        if len(binaries) > 0:
            self.process(binaries)
        else:
            click.echo("Nothing to do!")

    def __start_subprocess__(self, job):
        job_name = os.path.basename(os.path.splitext(job)[0])
        if self.log_dir is not None:
            log_path = os.path.join(self.log_dir, "{}.log".format(job_name))
            log_file = open(log_path, "w")
        else:
            log_file = open(os.devnull, "w")
        sub_process = subprocess.Popen(
            [job],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp,
        )
        self.logbook[sub_process.pid] = [sub_process, job, log_file]
        self.running_pid.append(sub_process.pid)
        return sub_process.pid

    def __clear_job__(self, pid):
        sub_process, job, log_file = self.logbook[pid]
        self.running_pid.remove(pid)
        if self.log_dir is not None:
            log_file.close()
        del self.logbook[pid]

    def register_sigint(self):
        if self._original_sigint is None:
            self._original_sigint = signal.getsignal(signal.SIGINT)

        def exit_gracefully(signum, frame):
            for pid in self.running_pid:
                os.kill(pid, signal.SIGSTOP)
            signal.signal(signal.SIGINT, self._original_sigint)
            try:
                quit = click.confirm("\nReally want to quit?")
            except click.Abort:
                quit = False
            if quit:
                self.__wait_rest__(self.log_dir is not None)
                sys.exit(0)
            else:
                click.echo("\nContinuing!")
                for pid in self.running_pid:
                    os.kill(pid, signal.SIGCONT)
            self.register_sigint()

        signal.signal(signal.SIGINT, exit_gracefully)


@click.command()
@click.argument("path", type=click.Path(exists=True, resolve_path=True))
@click.option("-j", "--n_jobs", default=1, help="Number of parallel jobs")
@click.option(
    "-p", "--binary_pattern", default="*.sh", help="Pattern of the binaries"
)
@click.option(
    "-l",
    "--log_path",
    default=None,
    type=click.Path(resolve_path=True),
    help="Path to a dir where the stdout/stderr should be saved",
)
@click.option("--resume/--no-resume", default=False)
def main(path, binary_pattern, n_jobs, log_path, resume):
    path = os.path.abspath(path)

    log_book = JobLogBook(n_jobs=n_jobs, log_dir=log_path)
    log_book.register_sigint()
    click.echo("Starting processing!")
    if resume:
        click.echo(
            "Resuming {} with max. {} parallel jobs!".format(path, n_jobs)
        )
        log_book.resume(path)
    else:
        binaries = list(glob.glob(os.path.join(path, binary_pattern)))
        log_book.process(binaries)


if __name__ == "__main__":
    main()
