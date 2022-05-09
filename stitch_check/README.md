# neut stitch check

Check every analysis has a stitched plate. Send slack alert for those without.

The binary in the repo is compiled for X86 linux.

## TODO:
- Cache `NE_available_strains` info to reduce db queries.

-------------


# milestones

Check for celebration-worthy milestones in terms of numbers of plates, wells
etc.

If run for the first time, then the database needs to be initted:

```bash
CREATE=1 ./milestones
```

then once the database has been created, you should omit the `CREATE=1` env
variable.

This checks the LIMS database and sends a slack notfication on a milestone
being reached. Milestones can be altered by changing the `neut_milestones.db`
sqlite file.
