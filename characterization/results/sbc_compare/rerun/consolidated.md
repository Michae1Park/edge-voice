# SBC STT latency — consolidated (30 runs per cell)

Model: Moonshine tiny-ko (quantized), `wav/rx_recorded_1.wav`, `measure_sbc.py --repeats 30`, partials off, private broker.
Per run = mean STT decode of the 3 timed segments (first segment excluded); 90 segments per cell. All values ms. Parsed from run.log and asserted equal to the JSON mean.

## Summary

| Board | Cores | Threads | Decode mean | p50 | p95 | min–max | std of run means | E2E mean | CER |
|---|---|---|---|---|---|---|---|---|---|
| Desktop | all 32 (unpinned) | single | **142** | 142 | 171 | 111–178 | 4 | 165 | 9.4% |
| Desktop | all 32 (unpinned) | multi | **135** | 120 | 227 | 82–407 | 32 | 157 | 9.4% |
| Desktop | 0-3 (2 P-cores + HT) | single | **187** | 190 | 218 | 153–221 | 3 | 209 | 9.4% |
| Desktop | 0-3 (2 P-cores + HT) | multi | **2325** | 2232 | 3360 | 1124–4033 | 334 | 2347 | 9.4% |
| Desktop | 0,2,4,6 (4 P-cores) | single | **189** | 192 | 218 | 155–239 | 3 | 211 | 9.4% |
| Desktop | 0,2,4,6 (4 P-cores) | multi | **2039** | 2042 | 2963 | 670–3619 | 350 | 2062 | 9.4% |
| OPI5 Plus | 4-7 (A76) | single | **2915** | 2952 | 3230 | 2482–3405 | 64 | 2939 | 9.4% |
| OPI5 Plus | 4-7 (A76) | multi | **2875** | 2953 | 3324 | 2286–3512 | 100 | 2899 | 9.4% |
| OPI5 Plus | 0-3 (A55) | single | **14376** | 14730 | 16009 | 12274–16204 | 202 | 23153 | 9.4% |
| OPI5 Plus | 0-3 (A55) | multi | **10489** | 10426 | 12402 | 6336–12682 | 278 | 15501 | 9.4% |
| RPi5 | 0-3 (all) | single | **1733** | 1770 | 1925 | 1497–1965 | 11 | 1756 | 9.4% |
| RPi5 | 0-3 (all) | multi | **1525** | 1564 | 1734 | 1237–1775 | 36 | 1548 | 9.4% |

## Notes

- **Desktop** = i9-14900K (hybrid: CPUs 0-15 P-core threads, 16-31 E-cores). **OPI5 Plus** = RK3588, A76 @ 2.256 GHz (`performance` governor, ~43 °C), A55 cores 0-3. **RPi5** = BCM2712 overclocked to 2.8 GHz. RPi5 and OPI5 use the same ONNX Runtime build (v1.23.2, `libonnxruntime-ab8c4363`).
- **Confinement:** desktop/RPi5 via `--cores` (sched_setaffinity). OPI5 via cgroup cpuset (`systemd-run -p AllowedCPUs=...`), so even the multi-thread runtime pool stays in the cluster. Pinned desktop multi runs a ~32-thread spinning pool on 4 cores, hence ~2 s.
- **Desktop unpinned rows vary by session.** Re-measured after a reboot: unpinned single-thread 178–235 ms (8 runs and 6 runs), pinned 0-3 156–186 ms. The 142 ms above was a favorable session. Bare decoder timing showed no pinning effect, and `OMP_NUM_THREADS=1` (torch VAD) did not change it. Prefer the pinned rows for cross-board comparison.
- **OPI5 A55 E2E >> decode:** decode exceeds the inter-segment gap, so segments queue.
- **OPI5 A76 vs RPi5:** the OPI5 is 1.68× slower, but the clock ratio explains only 1.24×. Software, clock and thermals are matched, so the residual is likely hardware (memory/cache) but unverified.

## Per-run decode mean

| Run | desktopall-single | desktopall-multi | desktop-single | desktop-multi | desktop4p-single | desktop4p-multi | opi5-a76-single | opi5-a76-multi | opi5-a55-single | opi5-a55-multi | rpi5-single | rpi5-multi |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 148 | 187 | 189 | 2381 | 195 | 2189 | 2886 | 2934 | 14603 | 10441 | 1742 | 1464 |
| 2 | 146 | 144 | 189 | 2422 | 182 | 2729 | 2981 | 2838 | 14384 | 10255 | 1725 | 1572 |
| 3 | 146 | 134 | 184 | 2336 | 185 | 2062 | 3010 | 2807 | 14422 | 10206 | 1726 | 1475 |
| 4 | 141 | 145 | 193 | 1780 | 188 | 2549 | 2886 | 2665 | 14432 | 10576 | 1727 | 1520 |
| 5 | 140 | 114 | 186 | 1992 | 188 | 2362 | 2917 | 2843 | 14363 | 10074 | 1728 | 1528 |
| 6 | 135 | 116 | 187 | 2344 | 190 | 2636 | 2910 | 2890 | 14508 | 10578 | 1743 | 1432 |
| 7 | 142 | 124 | 191 | 2433 | 187 | 2533 | 2888 | 2760 | 14470 | 10574 | 1742 | 1491 |
| 8 | 140 | 125 | 187 | 2149 | 190 | 2666 | 2804 | 2991 | 14633 | 10592 | 1718 | 1550 |
| 9 | 147 | 218 | 191 | 2617 | 185 | 1882 | 2848 | 2901 | 14063 | 10359 | 1724 | 1477 |
| 10 | 139 | 137 | 189 | 2151 | 196 | 2070 | 2854 | 2847 | 14051 | 10903 | 1740 | 1593 |
| 11 | 139 | 129 | 190 | 2724 | 187 | 2009 | 2947 | 2766 | 14459 | 10999 | 1730 | 1564 |
| 12 | 140 | 117 | 188 | 2791 | 186 | 2455 | 2927 | 2971 | 14373 | 10426 | 1737 | 1535 |
| 13 | 147 | 127 | 185 | 2457 | 190 | 2436 | 2953 | 2877 | 14434 | 10828 | 1734 | 1545 |
| 14 | 139 | 149 | 186 | 2402 | 189 | 1766 | 2846 | 2904 | 14491 | 10590 | 1736 | 1510 |
| 15 | 148 | 110 | 177 | 2798 | 189 | 1728 | 2909 | 2864 | 14534 | 10392 | 1738 | 1516 |
| 16 | 139 | 114 | 183 | 2155 | 190 | 2037 | 2885 | 2807 | 14736 | 10658 | 1724 | 1504 |
| 17 | 144 | 108 | 187 | 2236 | 187 | 1711 | 2901 | 2764 | 13999 | 10504 | 1726 | 1528 |
| 18 | 141 | 232 | 190 | 2188 | 186 | 1480 | 3028 | 2967 | 14192 | 10462 | 1770 | 1568 |
| 19 | 152 | 129 | 187 | 2458 | 188 | 1915 | 2917 | 2859 | 14285 | 10534 | 1736 | 1515 |
| 20 | 137 | 116 | 187 | 2840 | 188 | 1755 | 2934 | 2901 | 14462 | 10460 | 1744 | 1512 |
| 21 | 142 | 173 | 185 | 1642 | 193 | 2029 | 2977 | 2670 | 14451 | 10362 | 1726 | 1571 |
| 22 | 141 | 132 | 183 | 2027 | 188 | 1775 | 2909 | 2842 | 14175 | 10943 | 1736 | 1536 |
| 23 | 141 | 103 | 186 | 2035 | 189 | 1847 | 2902 | 3076 | 13944 | 10752 | 1722 | 1517 |
| 24 | 141 | 114 | 183 | 2642 | 190 | 1757 | 2951 | 2794 | 14111 | 10517 | 1717 | 1525 |
| 25 | 135 | 111 | 186 | 2033 | 191 | 1815 | 2870 | 2930 | 14499 | 10486 | 1719 | 1516 |
| 26 | 143 | 143 | 188 | 2427 | 192 | 2060 | 3123 | 2877 | 14594 | 10134 | 1729 | 1558 |
| 27 | 149 | 111 | 187 | 2684 | 185 | 1877 | 2853 | 2889 | 14519 | 10415 | 1735 | 1562 |
| 28 | 139 | 117 | 191 | 2745 | 185 | 1719 | 2864 | 3094 | 14488 | 10457 | 1736 | 1481 |
| 29 | 149 | 165 | 190 | 1581 | 191 | 1724 | 2872 | 3014 | 14138 | 10629 | 1734 | 1530 |
| 30 | 147 | 104 | 188 | 2287 | 192 | 1613 | 2896 | 2903 | 14469 | 9573 | 1754 | 1547 |
