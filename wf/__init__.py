from pathlib import Path
import subprocess

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import (LatchAuthor, LatchDir, LatchFile, LatchMetadata,
                        LatchParameter, LatchRule)


@small_task(retries=0)
def downsample_task(
    r1: LatchFile,
    r2: LatchFile,
    out_dir: str,
    out_reads: int
) -> (LatchFile, LatchFile):

    r1_name, r2_name = (Path(r).name for r in (r1, r2))
    out_r1, out_r2 = (Path(f"ds_{n}").resolve() for n in (r1_name, r2_name))

    _reformat_cmd = [
        "bbmap/reformat.sh",
        f"in1={r1.local_path}",
        f"in2={r2.local_path}",
        f"out1={str(out_r1)}",
        f"out2={str(out_r2)}",
        f"samplereadstarget={out_reads}"
        ]

    subprocess.run(_reformat_cmd)

    return (
            LatchFile(
                str(out_r1),
                f"latch:///downsampled/{out_dir}/{out_r1.name}"
        ),
            LatchFile(
                str(out_r2),
                f"latch:///downsampled/{out_dir}/{out_r2.name}"
        )
    )


metadata = LatchMetadata(
    display_name="downsample reads",
    author=LatchAuthor(
        name="James McGann",
        email="jpaulmcgann@gmail.com",
        github="github.com/jpmcga",
    ),
    repository="https://github.com/jpmcga/spatial-atacseq_latch/",
    parameters={
        "r1": LatchParameter(
            display_name="read 1",
            description="either fasta or fastq",
            batch_table_column=True,
        ),
        "r2": LatchParameter(
            display_name="read 2",
            description="either fasta or fastq",
            batch_table_column=True,
        ),
        "out_dir": LatchParameter(
            display_name="out dir",
            description="name of subdir in downsample/",
            batch_table_column=True,
            rules=[
                LatchRule(
                    regex="^[^/].*",
                    message="run id cannot start with a '/'"
                )
            ]
        ),
        "out_reads": LatchParameter(
            display_name="out reads",
            description="exact number of OUTPUT reads (or pairs) desired",
            batch_table_column=True,
        ),
    },
)


@workflow(metadata)
def downsample(
    r1: LatchFile,
    r2: LatchFile,
    out_dir: str,
    out_reads: int
) -> (LatchFile, LatchFile):
    """Quick workflow for downsampling paired-end reads.

    downsample reads
    ----

    Quick workflow for downsampling paired-end reads with bbmap.reformat.sh.
    Assumes fasta/q format; returns filtered reads to directory /outputs.
    For more info see https://github.com/BioInfoTools/BBMap/blob/master/sh/reformat.sh
    """

    
    return downsample_task(
        r1=r1,
        r2=r2,
        out_dir=out_dir,
        out_reads=out_reads
    )


LaunchPlan(
    downsample,
    "Test Data",
    {
        "r1" : LatchFile("latch:///BASESPACE_IMPORTS/projects/PL000121/D01033_NG01681_L1/D01033_NG01681_S3_L001_R1_001.fastq.gz"),
        "r2" : LatchFile("latch:///BASESPACE_IMPORTS/projects/PL000121/D01033_NG01681_L1/D01033_NG01681_S3_L001_R2_001.fastq.gz"),
        "out_dir" : "D01033_NG01681",
        "out_reads" : 50000000
    }
)

if __name__ == "__main__":
   downsample_task(
    r1="latch:///BASESPACE_IMPORTS/projects/PL000162/B00508_NG02348_L1/B00508_NG02348_S8_L001_R1_001.fastq.gz",
    r2="latch:///BASESPACE_IMPORTS/projects/PL000162/B00508_NG02348_L1/B00508_NG02348_S8_L001_R2_001.fastq.gz",
    out_dir="JA_ds_B00508_NG02348",
    out_reads=18000000
   )