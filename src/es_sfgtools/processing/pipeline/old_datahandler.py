TARGET_MAP = {
    AssetType.QCPIN: {AssetType.SHOTDATA: sv3_ops.dev_qcpin_to_shotdata},
    AssetType.NOVATEL: {
        AssetType.RINEX: gnss_ops.novatel_to_rinex,
        AssetType.POSITION: sv2_ops.novatel_to_positiondf,
    },
    AssetType.RINEX: {AssetType.KIN: gnss_ops.rinex_to_kin},
    AssetType.KIN: {AssetType.GNSS: gnss_ops.kin_to_gnssdf},
    AssetType.SONARDYNE: {AssetType.ACOUSTIC: sv2_ops.sonardyne_to_acousticdf},
    AssetType.MASTER: {AssetType.SITECONFIG: site_ops.masterfile_to_siteconfig},
    AssetType.LEVERARM: {AssetType.ATDOFFSET: site_ops.leverarmfile_to_atdoffset},
    AssetType.SEABIRD: {AssetType.SVP: site_ops.seabird_to_soundvelocity},
    AssetType.NOVATEL770: {AssetType.RINEX: gnss_ops.novatel_to_rinex},
    AssetType.DFOP00: {AssetType.SHOTDATA: sv3_ops.dev_dfop00_to_shotdata},
}


# Reverse the target map so we can get the parent type from the child type keys
# Format should be {child_type:[parent_type_0,parent_type_1,..]}
SOURCE_MAP = {}
for parent, children in TARGET_MAP.items():
    for child in children.keys():
        if not SOURCE_MAP.get(child, []):
            SOURCE_MAP[child] = []
        SOURCE_MAP[child].append(parent)


class MergeFrequency(Enum):
    HOUR = "h"
    DAY = "D"


class DEV_DH_DEP:
    # placeholder for temporarily deprecated functions
    def pipeline_sv2(self, override: bool = False, show_details: bool = False):
        self._process_data_graph(
            AssetType.POSITION, override=override, show_details=show_details
        )
        self._process_data_graph(
            AssetType.ACOUSTIC, override=override, show_details=show_details
        )
        self._process_data_graph(
            AssetType.RINEX, override=override, show_details=show_details
        )
        position_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(
            source=AssetType.POSITION, override=override
        )
        acoustic_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(
            source=AssetType.ACOUSTIC, override=override
        )
        rinex_ma_list: List[MultiAssetEntry] = self.dev_group_session_data(
            source=AssetType.RINEX, override=override
        )
        shot_ma_list: List[MultiAssetEntry] = sv2_ops.multiasset_to_shotdata(
            acoustic_assets=acoustic_ma_list,
            position_assets=position_ma_list,
            working_dir=self.proc_dir,
        )
        processed_rinex_kin: Tuple[
            List[AssetEntry | MultiAssetEntry], List[AssetEntry | MultiAssetEntry]
        ] = self._process_data_link(
            target=AssetType.KIN,
            source=AssetType.RINEX,
            override=override,
            parent_entries=rinex_ma_list,
            show_details=show_details,
        )
        processed_kin_gnss: Tuple[
            List[AssetEntry | MultiAssetEntry], List[AssetEntry | MultiAssetEntry]
        ] = self._process_data_link(
            target=AssetType.GNSS,
            source=AssetType.KIN,
            override=override,
            parent_entries=processed_rinex_kin[1],
            show_details=show_details,
        )

    def get_parent_stack(self, child_type: AssetType) -> List[AssetType]:
        """
        Get a list of parent types for a given child type.

        Args:
            child_type (Union[FILE_TYPE,DATA_TYPE]): The child type.

        Returns:
            List[Union[FILE_TYPE,DATA_TYPE]]: A list of parent types.
        """
        stack = [child_type]
        pointer = 0
        while pointer < len(stack):
            parents: List[AssetType] = SOURCE_MAP.get(stack[pointer], [])
            for parent in parents:
                stack.append(parent)
            pointer += 1
        return stack[::-1]

    def get_child_stack(self, parent_type: AssetType) -> List[AssetType]:

        stack = [parent_type]
        pointer = 0
        while pointer < len(stack):
            children: List[Union[FILE_TYPE, DATA_TYPE]] = list(
                TARGET_MAP.get(stack[pointer], {}).keys()
            )
            for child in children:
                stack.append(child)
            pointer += 1
        return stack

    @staticmethod
    def _partial_function(
        process_func: Callable,
        parent: AssetEntry,
        inter_dir: Path,
        pride_dir: Path,
        show_details: bool = False,
    ) -> Callable:
        match process_func:
            case gnss_ops.rinex_to_kin:

                process_func_p = partial(
                    process_func,
                    writedir=inter_dir,
                    pridedir=pride_dir,
                    site=parent.station,
                    show_details=show_details,
                )
            case gnss_ops.novatel_to_rinex:
                try:
                    year = str(parent.timestamp_data_start.year)[2:]
                except:
                    year = None
                process_func_p = partial(
                    process_func,
                    writedir=inter_dir,
                    site=parent.station,
                    year=year,
                    show_details=show_details,
                )
            # case gnss_ops.qcpin_to_novatelpin:
            #     process_func_p = partial(process_func, writedir=inter_dir)

            case _:
                process_func_p = process_func
        return process_func_p

    @staticmethod
    def _process_targeted(
        parent: AssetEntry | MultiAssetEntry,
        child_type: AssetType,
        inter_dir: Path,
        proc_dir: Path,
        pride_dir: Path,
        show_details: bool = False,
    ) -> Union[Tuple[AssetEntry, AssetEntry, str], Tuple[None, None, str]]:

        response = f"Processing {parent.local_path} ({parent.id}) of Type {parent.type} to {child_type.value}\n"

        # Get the processing function that converts the parent entry to the child entry
        try:
            process_func = TARGET_MAP.get(parent.type).get(child_type)
        except KeyError:
            response += f"  No processing function found for {parent.type} to {child_type.value}\n"
            logger.error(response)
            return None, None, response

        process_func_partial = DataHandler._partial_function(
            process_func=process_func,
            parent=parent,
            inter_dir=inter_dir,
            pride_dir=pride_dir,
            show_details=show_details,
        )

        try:
            processed = process_func_partial(parent)
            if processed is None:
                raise Exception(f"Processing failed for {parent.id}")
        except Exception as e:
            response += f"{process_func.__name__} failed with error: {e}"
            logger.error(response)
            return None, None, ""

        local_path = None
        timestamp_data_start = None
        timestamp_data_end = None
        match child_type:
            case (
                AssetType.GNSS
                | AssetType.ACOUSTIC
                | AssetType.POSITION
                | AssetType.SHOTDATA
            ):
                local_path = proc_dir / f"{parent.id}_{child_type.value}.csv"
                processed.to_csv(local_path, index=False)

                # handle the case when the child timestamp is None
                if pd.isna(parent.timestamp_data_end):
                    for col in processed.columns:
                        if pd.api.types.is_datetime64_any_dtype(processed[col]):
                            timestamp_data_start = processed[col].min()
                            timestamp_data_end = processed[col].max()
                            break
                else:
                    timestamp_data_start = parent.timestamp_data_start
                    timestamp_data_end = parent.timestamp_data_end

                local_path = (
                    proc_dir
                    / f"{parent.id}_{child_type.value}_{timestamp_data_start.date().isoformat()}.csv"
                )
                processed.to_csv(local_path, index=False)

                if isinstance(parent, MultiAssetEntry):
                    schema = MultiAssetEntry
                else:
                    schema = AssetEntry

                processed = schema(
                    local_path=local_path,
                    type=child_type,
                    parent_id=parent.id,
                    timestamp_data_start=timestamp_data_start,
                    timestamp_data_end=timestamp_data_end,
                    network=parent.network,
                    station=parent.station,
                    campaign=parent.campaign,
                )

            case AssetType.RINEX:
                local_path = processed.local_path
                timestamp_data_start = processed.timestamp_data_start
                timestamp_data_end = processed.timestamp_data_end

            case AssetType.KIN:
                local_path = processed.local_path

            case AssetType.SITECONFIG | AssetType.ATDOFFSET:
                local_path = proc_dir / f"{parent.id}_{child_type.value}.json"
                with open(local_path, "w") as f:
                    f.write(processed.model_dump_json())

            case AssetType.NOVATELPIN:
                local_path = inter_dir / f"{parent.id}_{child_type.value}.txt"
                processed.local_path = local_path
                processed.write(dir=local_path.parent)

            case _:
                local_path = None
                pass

        if (
            pd.isna(parent.timestamp_data_start)
            and processed.timestamp_data_start is not None
        ):
            parent.timestamp_data_start = processed.timestamp_data_start
            parent.timestamp_data_end = processed.timestamp_data_end
            response += f"  Discovered timestamp: {timestamp_data_start} for parent {parent.type.value} uuid {parent.id}\n"

        if not local_path.exists():
            response += f"  {child_type.value} not created for {parent.id}\n"
            logger.error(response)
            return None, parent, response

        return processed, parent, response

    def _process_entries(
        self,
        parent_entries: List[AssetEntry | MultiAssetEntry],
        child_type: AssetType,
        show_details: bool = False,
    ) -> List[
        Tuple[List[AssetEntry | MultiAssetEntry], List[AssetEntry | MultiAssetEntry]]
    ]:

        process_func_partial = partial(
            self._process_targeted,
            child_type=child_type,
            inter_dir=self.inter_dir,
            proc_dir=self.proc_dir,
            pride_dir=self.pride_dir,
        )
        parent_data_list = []
        child_data_list = []
        source_values = list(
            set([x.type.value for x in parent_entries if x is not None])
        )
        with multiprocessing.Pool() as pool:
            results = pool.imap(process_func_partial, parent_entries)
            for child_data, parent_data, response in tqdm(
                results,
                total=len(parent_entries),
                desc=f"Processing {source_values} To {child_type.value}",
            ):
                if parent_data is not None and child_data is not None:
                    if (
                        parent_data.timestamp_data_start is None
                        and child_data.timestamp_data_start is not None
                    ):
                        parent_data.timestamp_data_start = (
                            child_data.timestamp_data_start
                        )
                        parent_data.timestamp_data_end = child_data.timestamp_data_end
                    parent_data_list.append(parent_data)
                    child_data_list.append(child_data)

        response = f"Processed {len(child_data_list)} Out of {len(parent_entries)} For {child_type.value}"
        logger.info(response)
        if show_details:
            print(response)
        return parent_data_list, child_data_list

    def _process_data_link(
        self,
        target: AssetType | MultiAssetEntry,
        source: AssetType | MultiAssetEntry,
        override: bool = False,
        parent_entries: Union[List[AssetEntry], List[MultiAssetEntry]] = None,
        show_details: bool = False,
    ) -> Tuple[List[AssetEntry | MultiAssetEntry], List[AssetEntry | MultiAssetEntry]]:
        """
        Process data from a source to a target.

        Args:
            target (Union[FILE_TYPE,DATA_TYPE]): The target data type.
            source (List[FILE_TYPE]): The source data types.
            override (bool): Whether to override existing child entries.

        Raises:
            Exception: If no matching data is found in the catalog.
        """
        # Get the parent entries
        if parent_entries is None:
            parent_entries = self.catalog.get_single_entries_to_process(
                network=self.network,
                station=self.station,
                campaign=self.campaign,
                parent_type=source,
                child_type=target,
                override=override,
            )

        if parent_entries is None:
            return [], []
        parent_data_list, child_data_list = self._process_entries(
            parent_entries=parent_entries, child_type=target, show_details=show_details
        )
        for parent_data, child_data in zip(parent_data_list, child_data_list):

            self.catalog.add_or_update(parent_data)
            if child_data is not None:
                self.catalog.add_or_update(child_data)

        return parent_data_list, child_data_list

    def _process_data_graph(
        self, child_type: AssetType, override: bool = False, show_details: bool = False
    ):

        msg = f"\nProcessing Upstream Data for {child_type.value}\n"
        logger.info(msg)
        if show_details:
            print(msg)

        processing_queue = self.get_parent_stack(child_type=child_type)
        while processing_queue:
            parent = processing_queue.pop(0)
            if parent != child_type:
                children: dict = TARGET_MAP.get(parent, {})
                children_to_process = [
                    k for k in children.keys() if k in processing_queue
                ]
                for child in children_to_process:
                    msg = f"\nProcessing {parent.value} to {child.value}"
                    logger.info(msg)
                    if show_details:
                        print(msg)

                    self._process_data_link(
                        target=child,
                        source=parent,
                        override=override,
                        show_details=show_details,
                    )
        msg = f"Processed Upstream Data for {child_type.value}\n"
        logger.info(msg)
        if show_details:
            print(msg)

    def _process_data_graph_forward(
        self, parent_type: AssetType, override: bool = False, show_details: bool = False
    ):

        processing_queue = [{parent_type: TARGET_MAP.get(parent_type)}]
        while processing_queue:
            # process each level of the child graph
            parent_targets = processing_queue.pop(0)
            parent_type = list(parent_targets.keys())[0]
            for child in parent_targets[parent_type].keys():

                self._process_data_link(
                    target=child,
                    source=parent_type,
                    override=override,
                    show_details=show_details,
                )
                child_targets = TARGET_MAP.get(child, {})
                if child_targets:
                    processing_queue.append({child: child_targets})
