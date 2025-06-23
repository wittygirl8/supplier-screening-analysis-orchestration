import asyncio
from app.core.utils.db_utils import *
import json
import ast
from app.schemas.logger import logger

import json

async def ownership_analysis(data, session):

    module_activation = False

    logger.info("Performing Ownership Structure Analysis... Started")

    kpi_area_module = "OWN"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    if not module_activation:
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "module_deactivated"}

    try:
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        OWN1A = kpi_template.copy()
        OWN1A["kpi_code"] = "OWN1A"
        OWN1A["kpi_definition"] = "Direct Shareholder With > 50% Ownership"  # TODO SET THRESHOLD

        required_columns = [
            "shareholders", "controlling_shareholders", "controlling_shareholders_type",
            "beneficial_owners", "beneficial_owners_intermediatory", "global_ultimate_owner",
            "global_ultimate_owner_type", "other_ultimate_beneficiary", "ultimately_owned_subsidiaries"
        ]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        shareholders = retrieved_data.get("shareholders", None)
        controlling_shareholders = retrieved_data.get("controlling_shareholders", None)

        # Check if all/any mandatory required data is None
        if all(var is None for var in [shareholders, controlling_shareholders]):
            logger.info(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        def is_valid_ownership(value):
            """Checks if the value is a valid number greater than 50."""
            if not value or value == "n.a.":
                return False
            try:
                return float(value) > 50
            except ValueError:
                return False

        # ---- PERFORM ANALYSIS LOGIC HERE ----
        def process_ownership(owners_list):
            """Processes the list of owners and checks for >50% ownership."""
            for owner in owners_list:
                total = owner.get("total_ownership", "n.a.")
                direct = owner.get("direct_ownership", "n.a.")

                if is_valid_ownership(total):
                    OWN1A["kpi_value"] = json.dumps(owner)
                    OWN1A["kpi_details"] = f"Shareholder {owner.get('name')} has total ownership of {total}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    return True  # Exit after finding the first valid owner

                if is_valid_ownership(direct):
                    OWN1A["kpi_value"] = json.dumps(owner)
                    OWN1A["kpi_details"] = f"Shareholder {owner.get('name')} has direct ownership of {direct}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    return True  # Exit after finding the first valid owner
            return False

        if controlling_shareholders and process_ownership(controlling_shareholders):
            pass
        elif shareholders and process_ownership(shareholders):
            pass

        own_kpis = [OWN1A]
        insert_status = await upsert_kpi("oval", own_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            logger.info(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            logger.error(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        logger.error(f"Error in module: {kpi_area_module}, {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def ownership_flag(data, session):
    logger.info("Performing Ownership Structure Analysis... Started")

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    ENABLE_AMR_ANALYSIS = False
    try:
        # --- Fetch exclusion list from DB ---
        try:
            db_exclusions_raw = await get_dynamic_ens_data(
                table_name="excluded_entities",
                required_columns=["name"],
                session=session
            )
            exclusion_list = [item["name"].strip().lower() for item in db_exclusions_raw if item.get("name")]
        except Exception as e:
            logger.error(f"Failed to fetch exclusion lists from DB: {str(e)}")
            exclusion_list = []

        def is_excluded_entity(entity):
            """Check if entity name is in exclusion list"""
            if not entity or not isinstance(entity, dict):
                return True  # Exclude invalid entities
            name = entity.get("name", "").strip().lower()
            return name in exclusion_list

        def is_significant_entity(entity):
            """Check if entity has significance=true"""
            if not entity or not isinstance(entity, dict):
                return False
            return entity.get("significance", False) is True

        def should_include_entity(entity):
            """Check if entity should be included (not excluded AND significant)"""
            return not is_excluded_entity(entity) and is_significant_entity(entity)

        def get_ownership_percentage_from_all_sources(entity_name, retrieved_data):
            """Improved version with better error handling"""
            if not entity_name:
                return ">25%"

            if not isinstance(retrieved_data, dict):
                return ">25%"

            # Normalize the search name
            search_name = entity_name.strip().lower()

            # Check all possible ownership sources
            sources = [
                retrieved_data.get("shareholders", []),
                retrieved_data.get("beneficial_owners_intermediatory", []),
                retrieved_data.get("global_ultimate_owner", []),
                retrieved_data.get("controlling_shareholders", [])
            ]

            for source in sources:
                if not isinstance(source, list):
                    continue

                for entity in source:
                    if not isinstance(entity, dict):
                        continue

                    entity_name = entity.get("name", "").strip().lower()
                    if entity_name == search_name:
                        direct_ownership = entity.get("direct_ownership")
                        if direct_ownership and str(direct_ownership).lower() not in ["", "n.a.", "na", "-", "n.a"]:
                            try:
                                return f"{float(direct_ownership):.0f}%"
                            except (ValueError, TypeError):
                                pass

            return ">25%"

        kpi_template = {
            "kpi_area": "",
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": '',
            "kpi_rating": "",
            "kpi_details": ""
        }

        SAN3A = kpi_template.copy()
        PEP3A = kpi_template.copy()

        SAN3A["kpi_area"] = "SAN"
        SAN3A["kpi_code"] = "SAN3A"
        SAN3A["kpi_definition"] = "Associated Corporate Group - Sanctions Exposure"

        PEP3A["kpi_area"] = "PEP"
        PEP3A["kpi_code"] = "PEP3A"
        PEP3A["kpi_definition"] = "Associated Corporate Group - PeP/SOE Exposure"

        if ENABLE_AMR_ANALYSIS:
            AMR2A = kpi_template.copy()
            AMR2A["kpi_area"] = "AMR"
            AMR2A["kpi_code"] = "AMR2A"
            AMR2A["kpi_definition"] = "Associated Corporate Group - Media Exposure"

        sanctions_watchlist_findings = []
        pep_findings = []
        sanctions_watchlist_details = []
        pep_details = []
        sanctions_watchlist_kpi_flag = False
        pep_kpi_flag = False
        if ENABLE_AMR_ANALYSIS:
            media_findings = []
            media_details = []
            media_kpi_flag = False

        required_columns = ["shareholders", "beneficial_owners", "global_ultimate_owner", "other_ultimate_beneficiary",
                            "ultimately_owned_subsidiaries","beneficial_owners_intermediatory", "controlling_shareholders"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                    session_id_value, session)
        retrieved_data = retrieved_data[0] if retrieved_data else {}

        # Filter entities: exclude those in exclusion list AND only include those with significance=true
        shareholders = [sh for sh in (retrieved_data.get("shareholders") or []) if should_include_entity(sh)]
        beneficial_owners = [bo for bo in (retrieved_data.get("beneficial_owners") or []) if should_include_entity(bo)]
        beneficial_owners_intermediatory = [boi for boi in
                                            (retrieved_data.get("beneficial_owners_intermediatory") or []) if
                                            should_include_entity(boi)]
        global_ultimate_owner = [guo for guo in (retrieved_data.get("global_ultimate_owner") or []) if
                                 should_include_entity(guo)]
        other_ultimate_beneficiary = [oub for oub in (retrieved_data.get("other_ultimate_beneficiary") or []) if
                                      should_include_entity(oub)]
        ultimately_owned_subsidiaries = [uos for uos in (retrieved_data.get("ultimately_owned_subsidiaries") or []) if
                                         should_include_entity(uos)]

        # Check if all/any mandatory required data is None
        if all(not lst for lst in [shareholders, beneficial_owners, global_ultimate_owner,
                                   other_ultimate_beneficiary, ultimately_owned_subsidiaries,
                                   beneficial_owners_intermediatory]):
            logger.info("ownership_flag Analysis... Completed With No Data After Exclusion and Significance Filtering")
            return {"ens_id": ens_id_value, "module": "OVAL", "status": "completed", "info": "no_data"}

        # ---- PERFORM ANALYSIS LOGIC HERE
        if global_ultimate_owner is not None:
            rel_type_str = "Global Ultimate Owner:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in global_ultimate_owner:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = sh.get("direct_ownership", "-")
                try:
                    ownership_percent = f"{float(direct_ownership):.0f}%"
                except (ValueError, TypeError):
                    ownership_percent = "N/A"
                name_with_ownership = f"{name} ({ownership_percent})"

                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "global_ultimate_owner"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if shareholders is not None:
            rel_type_str = "Shareholders:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in shareholders:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = sh.get("direct_ownership", "-")
                try:
                    ownership_percent = f"{float(direct_ownership):.0f}%"
                except (ValueError, TypeError):
                    ownership_percent = "N/A"
                name_with_ownership = f"{name} ({ownership_percent})"

                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "shareholders"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if beneficial_owners is not None:
            rel_type_str = " Beneficials Owners [Persons]:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in beneficial_owners:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = get_ownership_percentage_from_all_sources(name, retrieved_data)
                if direct_ownership == ">25%":
                    ownership_percent = ">25%"
                else:
                    try:
                        ownership_percent = f"{float(direct_ownership):.0f}%"
                    except (ValueError, TypeError):
                        ownership_percent = ">25%"
                name_with_ownership = f"{name} ({ownership_percent})"
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "beneficial_owners"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))
        #
        if other_ultimate_beneficiary is not None:
            rel_type_str = " Beneficials Owners [Company]:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in other_ultimate_beneficiary:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = get_ownership_percentage_from_all_sources(name, retrieved_data)
                if direct_ownership == ">25%":
                    ownership_percent = ">25%"
                else:
                    try:
                        ownership_percent = f"{float(direct_ownership):.0f}%"
                    except (ValueError, TypeError):
                        ownership_percent = ">25%"
                name_with_ownership = f"{name} ({ownership_percent})"
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "other_ultimate_beneficiary"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        #beneficial owners intermediatory

        if beneficial_owners_intermediatory is not None:
            rel_type_str = "Beneficial Owners [Intermediatory]:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in beneficial_owners_intermediatory:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = get_ownership_percentage_from_all_sources(name, retrieved_data)
                if direct_ownership == ">25%":
                    ownership_percent = ">25%"
                else:
                    try:
                        ownership_percent = f"{float(direct_ownership):.0f}%"
                    except (ValueError, TypeError):
                        ownership_percent = ">25%"
                name_with_ownership = f"{name} ({ownership_percent})"
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "beneficial_owners_intermediatory"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if ultimately_owned_subsidiaries is not None:
            rel_type_str = "Ultimately Owned Subsidiaries:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in ultimately_owned_subsidiaries:
                name = sh.get("name", None)
                if name.lower() == "self owned":
                    continue

                direct_ownership = sh.get("direct_ownership", "-")
                try:
                    ownership_percent = f"{float(direct_ownership):.0f}%"
                except (ValueError, TypeError):
                    ownership_percent = "N/A"
                name_with_ownership = f"{name} ({ownership_percent})"

                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "ultimately_owned_subsidiaries"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name_with_ownership)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name_with_ownership)
                if ENABLE_AMR_ANALYSIS and media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name_with_ownership)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if ENABLE_AMR_ANALYSIS and media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if sanctions_watchlist_kpi_flag:
            total_san_count = len(sanctions_watchlist_findings)
            SAN3A["kpi_flag"] = True
            SAN3A["kpi_rating"] = "High"
            SAN3A["kpi_details"] = "Sanctions or watchlist exposure identified among following members of the corporate group:\n" + "\n \n".join(
                sanctions_watchlist_details) + ("\n...& " + str(total_san_count - 5) + " more findings" if total_san_count > 6 else "")
            kpi_dict = {
                "count": total_san_count if total_san_count < 6 else "5 or more",
                "findings": sanctions_watchlist_findings
            }
            SAN3A["kpi_value"] = json.dumps(kpi_dict)

            san_kpis = [SAN3A]
            insert_status = await upsert_kpi("sape", san_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                logger.info("SAN3A Analysis... Completed Successfully")
            else:
                logger.error(insert_status)

        if pep_kpi_flag:
            total_pep_count = len(pep_findings)
            PEP3A["kpi_flag"] = True
            PEP3A["kpi_rating"] = "High"
            PEP3A["kpi_details"] = "PEP exposure identified among following members of the corporate group:\n" + "\n \n".join(
                pep_details) + ("\n...& " + str(total_pep_count - 5) + " more findings" if total_pep_count > 5 else "")
            kpi_dict = {
                "count": total_pep_count if total_pep_count < 6 else "5 or more",
                "findings": pep_findings
            }
            PEP3A["kpi_value"] = json.dumps(kpi_dict)

            pep_kpis = [PEP3A]
            insert_status = await upsert_kpi("sown", pep_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                logger.info(f"PEP3A Analysis... Completed Successfully")
            else:
                logger.error(insert_status)

        if ENABLE_AMR_ANALYSIS and media_kpi_flag:
            total_med_count = len(media_findings)
            AMR2A["kpi_flag"] = True
            AMR2A["kpi_rating"] = "High"
            AMR2A["kpi_details"] = "Possible Negative Media exposure identified among following members of the corporate group:\n" + "\n \n".join(
                media_details) + ("\n...& " + str(total_med_count - 5) + " more findings" if total_med_count > 5 else "")
            kpi_dict = {
                "count": total_med_count if total_med_count < 6 else "5 or more",
                "findings": media_findings
            }
            AMR2A["kpi_value"] = json.dumps(kpi_dict)

            amr_kpis = [AMR2A]
            insert_status = await upsert_kpi("rfct", amr_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                logger.info(f"AMR2A Analysis... Completed Successfully")
            else:
                logger.error(insert_status)

        return {"ens_id": ens_id_value, "module": "OVAL EXTRA", "status": "completed", "info": "analysed"}


    except Exception as e:
        logger.error(f"Error in module: OVAL EXTRA, {str(e)}")
        return {"ens_id": ens_id_value, "module": "OVAL EXTRA", "status": "failure", "info": str(e)}