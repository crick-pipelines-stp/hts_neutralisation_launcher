# neut stitch check

Check every analysis has a stitched plate. Send slack alert for those without.

## TODO:
- Titration plates
- Cache `NE_available_strains` info to reduce db queries.
- Check `finished_at` times for recently launched tasks, maybe false positives.
